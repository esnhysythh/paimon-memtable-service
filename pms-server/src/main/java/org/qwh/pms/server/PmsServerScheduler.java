package org.qwh.pms.server;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.qwh.pms.core.bucket.BucketStateSnapshot;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.bucket.SinkFlightSnapshot;
import org.qwh.pms.core.bucket.operation.CompactionResult;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.EvictionResult;
import org.qwh.pms.core.bucket.operation.FlushResult;
import org.qwh.pms.core.bucket.operation.FreezeResult;
import org.qwh.pms.core.bucket.operation.SinkOperationResult;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.storage.SSTState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates background and synchronous maintenance for one PMS table.
 * 为单个 PMS 表协调后台维护与同步维护操作.
 *
 * <p>The scheduler owns two independent single-threaded execution domains: Flush only advances
 * immutable MemTables to local SSTs, while maintenance serializes visibility fences, Paimon Sink,
 * local compaction, and eviction. Core snapshots remain the source of truth; the scheduler keeps
 * only the active Paimon visibility fence and worker lifecycle/diagnostic state.
 *
 * <p>调度器拥有两个相互独立的单线程执行域: Flush 仅负责将 ImmutableMemTable 推进为本地
 * SST;Maintenance 则串行执行可见性 fence、Paimon Sink、本地 compact 和淘汰.Core 快照始终
 * 是事实来源;调度器只保存当前生效的 Paimon 可见性 fence, 以及 worker 生命周期和诊断状态.
 *
 * <p>Automatic reconciliation is sliced by {@link #MAX_ACTIONS_PER_RUN} for fairness. Management
 * commands instead drain a fixed sequence target to completion (or fail on no progress), so their
 * semantics are not truncated by the background-worker budget.
 *
 * <p>自动调和按照 {@link #MAX_ACTIONS_PER_RUN} 划分执行片段, 以保证调度公平性.管理命令则会
 * 持续推进固定的 sequence 目标, 直至完成;如果无法取得进展则明确失败, 因此其语义不会被后台
 * worker 的单轮动作预算截断.
 */
public final class PmsServerScheduler implements AutoCloseable {
    static final String ACTION_LOG_MARKER = "PMS_SCHEDULER_ACTION";
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerScheduler.class);
    private static final AtomicInteger THREAD_ID = new AtomicInteger();
    private static final int MAX_ACTIONS_PER_RUN = 64;

    private final Operations operations;
    private final PmsSchedulerConfig config;
    private final SchedulerTimeSource timeSource;
    private final ScheduledExecutorService flushExecutor;
    private final ScheduledExecutorService maintenanceExecutor;
    private final AtomicBoolean flushQueuedOrRunning = new AtomicBoolean();
    private final AtomicBoolean maintenanceQueuedOrRunning = new AtomicBoolean();
    private final AtomicBoolean flushRerunRequested = new AtomicBoolean();
    private final AtomicBoolean maintenanceRerunRequested = new AtomicBoolean();
    private final ConcurrentHashMap<String, AtomicLong> actionSuccessCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> actionNoopCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> actionFailureCounts = new ConcurrentHashMap<>();

    private volatile boolean running;
    private volatile boolean draining;
    private volatile boolean closed;
    private volatile ScheduledFuture<?> flushPeriodicTask;
    private volatile ScheduledFuture<?> maintenancePeriodicTask;
    private volatile boolean flushRunning;
    private volatile boolean maintenanceRunning;
    // The only cross-operation plan state. Zero means no visibility fence is active.
    // 唯一跨 Operation 保留的计划状态; 零表示当前没有生效的可见性 fence.
    private volatile long pendingPaimonFenceSequenceId;
    private volatile long flushRetryNotBeforeMillis;
    private volatile long maintenanceRetryNotBeforeMillis;
    private volatile String lastFlushStartedAt;
    private volatile String lastFlushCompletedAt;
    private volatile long lastFlushDurationMs;
    private volatile long flushRunCount;
    private volatile String lastMaintenanceStartedAt;
    private volatile String lastMaintenanceCompletedAt;
    private volatile long lastMaintenanceDurationMs;
    private volatile long maintenanceRunCount;
    private volatile String lastMaintenanceAction;
    private volatile String lastMaintenanceReason;
    private volatile String lastErrorAt;
    private volatile String lastErrorWorker;
    private volatile String lastErrorAction;
    private volatile String lastErrorMessage;

    public PmsServerScheduler(PmsTableService service, PmsSchedulerConfig config) {
        this(requireService(service), config, SchedulerTimeSource.SYSTEM);
    }

    PmsServerScheduler(Operations operations, PmsSchedulerConfig config, SchedulerTimeSource timeSource) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.config = config == null ? PmsSchedulerConfig.defaults() : config;
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
        this.flushExecutor = newWorker("flush");
        this.maintenanceExecutor = newWorker("maintenance");
    }

    private static PmsTableService requireService(PmsTableService service) {
        return Objects.requireNonNull(service, "service must not be null");
    }

    private static ScheduledExecutorService newWorker(String role) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(
                runnable,
                "pms-server-scheduler-" + role + "-" + THREAD_ID.incrementAndGet()
            );
            thread.setDaemon(true);
            return thread;
        });
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        ensureOpen();
        if (draining) {
            throw new IllegalStateException("PMS server scheduler is draining");
        }
        if (!config.enabled()) {
            LOG.info("PMS server scheduler disabled");
            return;
        }
        running = true;
        flushPeriodicTask = flushExecutor.scheduleWithFixedDelay(
            this::requestFlush,
            0,
            config.flushReconcileIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        maintenancePeriodicTask = maintenanceExecutor.scheduleWithFixedDelay(
            this::requestMaintenance,
            0,
            config.maintenanceReconcileIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        LOG.info(
            "PMS server scheduler started: flushReconcileIntervalMs={}, maintenanceReconcileIntervalMs={}, visibilityMaxDelayMs={}",
            config.flushReconcileIntervalMs(),
            config.maintenanceReconcileIntervalMs(),
            config.visibilityMaxDelayMs()
        );
    }

    /** Stops background reconciliation while keeping both executors available for final drain. */
    public synchronized void beginDrain() {
        ensureOpen();
        if (draining) {
            return;
        }
        draining = true;
        running = false;
        cancel(flushPeriodicTask);
        cancel(maintenancePeriodicTask);
        flushPeriodicTask = null;
        maintenancePeriodicTask = null;
        flushRerunRequested.set(false);
        maintenanceRerunRequested.set(false);
        LOG.info("PMS server scheduler entered draining mode; background reconciliation stopped");
    }

    /** Freezes the current write boundary and synchronously flushes every immutable covering it. */
    public void flushToCurrent() {
        ensureOpen();
        FreezeResult freeze = callMaintenance(() -> executeFreeze("MANUAL_FLUSH", operations.stateSnapshot()));
        long fence = freeze.fenceSequenceId();
        callFlush(() -> {
            drainFlushTarget("MANUAL_FLUSH", fence);
            BucketStateSnapshot state = operations.stateSnapshot();
            if (state.lastFlushedSequenceId() < fence) {
                throw new IllegalStateException(
                    "manual Flush did not reach fence " + fence + ", lastFlushed=" + state.lastFlushedSequenceId()
                );
            }
            return null;
        });
    }

    /** Synchronously sinks the stable NEW prefix visible when this command starts. */
    public void sinkAvailable() {
        ensureOpen();
        callMaintenance(() -> {
            BucketStateSnapshot initial = operations.stateSnapshot();
            long targetSequenceId = initial.newSSTMaxSequenceId();
            drainSinkTarget("MANUAL_SINK", targetSequenceId);
            return null;
        });
    }

    /** Establishes one final fence and synchronously Flushes and Sinks through it. */
    public void drainToPaimon() {
        ensureOpen();
        FreezeResult freeze = callMaintenance(() -> executeFreeze("DRAIN_TO_PAIMON", operations.stateSnapshot()));
        long fence = freeze.fenceSequenceId();
        callFlush(() -> {
            drainFlushTarget("DRAIN_TO_PAIMON", fence);
            return null;
        });
        callMaintenance(() -> {
            drainSinkTarget("DRAIN_TO_PAIMON", fence);
            return null;
        });
    }

    /**
     * Runs both reconciliation paths synchronously for management and deterministic tests.
     * 为管理操作和确定性测试同步执行两条调和路径.
     *
     * <p>The first maintenance pass may establish a visibility fence, Flush materializes that
     * fence, and the second maintenance pass can then Sink it. The ordering is therefore part of
     * the method's behavior rather than three interchangeable wakeups.
     *
     * <p>第一次 Maintenance 可能建立可见性 fence, 随后 Flush 将该 fence 对应的数据物化为 SST, 
     * 第二次 Maintenance 才能继续完成 Sink.因此, 这一执行顺序属于方法语义的一部分, 不能将其
     * 视为三次可以任意互换的唤醒.
     */
    public void reconcileNow() {
        ensureOpen();
        callMaintenance(() -> {
            reconcileMaintenance(false);
            return null;
        });
        callFlush(() -> {
            drainFlush("RECONCILE_NOW", false);
            return null;
        });
        callMaintenance(() -> {
            reconcileMaintenance(false);
            return null;
        });
    }

    public Map<String, Object> state() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", config.enabled());
        result.put("running", running);
        result.put("draining", draining);
        result.put("flushReconcileIntervalMs", config.flushReconcileIntervalMs());
        result.put("maintenanceReconcileIntervalMs", config.maintenanceReconcileIntervalMs());
        result.put("failureRetryDelayMs", config.failureRetryDelayMs());
        result.put("visibilityMaxDelayMs", config.visibilityMaxDelayMs());
        result.put("newSstMaxCount", config.newSstMaxCount());
        result.put("sinkedSstMaxCount", config.sinkedSstMaxCount());
        result.put("pendingPaimonFenceSequenceId", pendingPaimonFenceSequenceId);
        result.put("flushRunning", flushRunning);
        result.put("lastFlushStartedAt", lastFlushStartedAt);
        result.put("lastFlushCompletedAt", lastFlushCompletedAt);
        result.put("lastFlushDurationMs", lastFlushDurationMs);
        result.put("flushRunCount", flushRunCount);
        result.put("flushRetryNotBeforeMillis", flushRetryNotBeforeMillis);
        result.put("maintenanceRunning", maintenanceRunning);
        result.put("lastMaintenanceStartedAt", lastMaintenanceStartedAt);
        result.put("lastMaintenanceCompletedAt", lastMaintenanceCompletedAt);
        result.put("lastMaintenanceDurationMs", lastMaintenanceDurationMs);
        result.put("maintenanceRunCount", maintenanceRunCount);
        result.put("maintenanceRetryNotBeforeMillis", maintenanceRetryNotBeforeMillis);
        result.put("lastMaintenanceAction", lastMaintenanceAction);
        result.put("lastMaintenanceReason", lastMaintenanceReason);
        result.put("actionSuccessCounts", countSnapshot(actionSuccessCounts));
        result.put("actionNoopCounts", countSnapshot(actionNoopCounts));
        result.put("actionFailureCounts", countSnapshot(actionFailureCounts));
        result.put("lastErrorAt", lastErrorAt);
        result.put("lastErrorWorker", lastErrorWorker);
        result.put("lastErrorAction", lastErrorAction);
        result.put("lastErrorMessage", lastErrorMessage);
        return result;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        running = false;
        draining = true;
        closed = true;
        shutdown(flushExecutor, "Flush");
        shutdown(maintenanceExecutor, "maintenance");
        LOG.info("PMS server scheduler stopped");
    }

    private void requestFlush() {
        if (!running || closed) {
            return;
        }
        // Periodic ticks and maintenance signals may race. Collapse them into the current pass plus
        // at most one follow-up pass instead of growing an unbounded executor queue.
        // 周期 tick 和 Maintenance signal 可能并发到达; 将它们合并为当前轮加至多一轮后续执行,
        // 避免 executor 队列无界增长.
        if (!flushQueuedOrRunning.compareAndSet(false, true)) {
            flushRerunRequested.set(true);
            return;
        }
        flushExecutor.execute(() -> {
            try {
                runFlushSafely();
            } finally {
                flushQueuedOrRunning.set(false);
                if (flushRerunRequested.getAndSet(false)) {
                    requestFlush();
                }
            }
        });
    }

    private void requestMaintenance() {
        if (!running || closed) {
            return;
        }
        // Use the same coalescing rule as the Flush worker; one rerun is enough because every pass
        // starts from a fresh BucketStateSnapshot.
        // 使用与 Flush worker 相同的信号合并规则; 由于每轮都会从最新 BucketStateSnapshot 开始,
        // 因此至多保留一次 rerun 即可.
        if (!maintenanceQueuedOrRunning.compareAndSet(false, true)) {
            maintenanceRerunRequested.set(true);
            return;
        }
        maintenanceExecutor.execute(() -> {
            try {
                runMaintenanceSafely();
            } finally {
                maintenanceQueuedOrRunning.set(false);
                if (maintenanceRerunRequested.getAndSet(false)) {
                    requestMaintenance();
                }
            }
        });
    }

    private void runFlushSafely() {
        if (!running || closed || nowMillis() < flushRetryNotBeforeMillis) {
            return;
        }
        long startedNanos = timeSource.monotonicNanos();
        flushRunning = true;
        lastFlushStartedAt = now();
        flushRunCount++;
        try {
            drainFlush("IMMUTABLE_BACKLOG", true);
            flushRetryNotBeforeMillis = 0;
        } catch (RuntimeException e) {
            recordWorkerFailure("flush", "FLUSH", e);
            flushRetryNotBeforeMillis = retryNotBefore();
            scheduleFlushRetry();
        } finally {
            lastFlushDurationMs = elapsedMillis(startedNanos);
            lastFlushCompletedAt = now();
            flushRunning = false;
        }
    }

    private void runMaintenanceSafely() {
        if (!running || closed || nowMillis() < maintenanceRetryNotBeforeMillis) {
            return;
        }
        long startedNanos = timeSource.monotonicNanos();
        maintenanceRunning = true;
        lastMaintenanceStartedAt = now();
        maintenanceRunCount++;
        try {
            reconcileMaintenance(true);
            maintenanceRetryNotBeforeMillis = 0;
        } catch (RuntimeException e) {
            recordWorkerFailure("maintenance", lastMaintenanceAction, e);
            maintenanceRetryNotBeforeMillis = retryNotBefore();
            scheduleMaintenanceRetry();
        } finally {
            lastMaintenanceDurationMs = elapsedMillis(startedNanos);
            lastMaintenanceCompletedAt = now();
            maintenanceRunning = false;
        }
    }

    private void drainFlush(String reason, boolean background) {
        for (int actions = 0; actions < MAX_ACTIONS_PER_RUN; actions++) {
            // A background slice yields promptly once draining starts. Synchronous callers pass
            // background=false and are governed by their fixed target instead.
            // 进入 draining 后, 后台执行片段应尽快让出; 同步调用方传入 background=false, 改由其
            // 固定目标约束执行过程.
            if (background && !running) {
                return;
            }
            BucketStateSnapshot state = operations.stateSnapshot();
            if (state.immutableMemTableCount() == 0) {
                return;
            }
            FlushResult result = executeFlush(state, reason);
            if (!result.progressed()) {
                return;
            }
            if (running) {
                requestMaintenance();
            }
        }
        if (background && running && operations.stateSnapshot().immutableMemTableCount() > 0) {
            requestFlush();
        }
    }

    private void drainFlushTarget(String reason, long targetSequenceId) {
        while (targetSequenceId > 0) {
            BucketStateSnapshot state = operations.stateSnapshot();
            if (state.lastFlushedSequenceId() >= targetSequenceId) {
                return;
            }
            if (state.immutableMemTableCount() == 0) {
                throw new IllegalStateException(
                    "Flush has no immutable MemTable before target " + targetSequenceId
                        + ", lastFlushed=" + state.lastFlushedSequenceId()
                );
            }
            FlushResult result = executeFlush(state, reason);
            if (!result.progressed()) {
                throw new IllegalStateException(
                    "Flush made no progress toward target " + targetSequenceId
                        + ", lastFlushed=" + state.lastFlushedSequenceId()
                );
            }
            if (running) {
                requestMaintenance();
            }
        }
    }

    private FlushResult executeFlush(BucketStateSnapshot state, String reason) {
        return executeAction(
            "flush",
            "FLUSH",
            reason,
            "immutableCount=" + state.immutableMemTableCount(),
            operations::flushImmutableMemTable,
            FlushResult::progressed
        );
    }

    private void reconcileMaintenance(boolean background) {
        // This is a priority reconciliation loop, not a precomputed plan. Every progressed action
        // invalidates the old snapshot, so the loop rereads state and starts again at priority 1.
        // 这是按优先级执行的调和循环, 而不是预先计算好的计划. 每个取得进展的动作都会使旧快照
        // 失效, 因此循环必须重新读取状态, 并从最高优先级重新判断.
        for (int actions = 0; actions < MAX_ACTIONS_PER_RUN; actions++) {
            if (background && !running) {
                return;
            }
            BucketStateSnapshot state = operations.stateSnapshot();

            // A durable prepared Sink must be resolved before starting any other SST maintenance.
            // Flush remains independent and may continue on its own worker.
            // durable prepared Sink 必须先得到解决, 才能启动其他 SST 维护; Flush 保持独立, 仍可
            // 在自己的 worker 上继续执行.
            if (state.sinkFlight().status() == SinkFlightSnapshot.Status.PREPARED_RETRY) {
                SinkOperationResult result = executePreparedRetry(state);
                if (!result.progressed()) {
                    return;
                }
                continue;
            }

            long fence = pendingPaimonFenceSequenceId;
            if (fence > 0) {
                // Persisted coverage satisfies the fence; discard controller state and reconsider
                // ordinary count maintenance from a new snapshot.
                // persisted boundary 覆盖 fence 后, 该目标即已满足;清除 controller 状态, 并基于
                // 新快照重新判断常规的数量维护.
                if (state.lastPersistedSequenceId() >= fence) {
                    pendingPaimonFenceSequenceId = 0;
                    continue;
                }
                // Sink cannot cover the fence until Flush has materialized its complete prefix.
                // 在 Flush 将 fence 对应的完整前缀物化为 SST 之前, Sink 无法覆盖该 fence.
                if (state.lastFlushedSequenceId() < fence) {
                    requestFlush();
                    return;
                }
                // One Sink call is deliberately bounded by batch count/bytes. Keep the same fence
                // across calls until lastPersistedSequenceId reaches it.
                // 单次 Sink 调用有意受到 batch 数量和字节数限制; 在 lastPersistedSequenceId 达到
                // fence 之前, 多次调用必须始终推进同一个 fence.
                SinkOperationResult result = executeSink(state, fence, "PAIMON_VISIBILITY_FENCE");
                if (!result.progressed()) {
                    return;
                }
                continue;
            }

            if (visibilityDue(state)) {
                // Freeze atomically captures the current write boundary. Newer writes land beyond
                // this fence and therefore do not prolong the current visibility objective.
                // Freeze 会原子捕获当前写入边界; 后续新写入位于 fence 之后, 因此不会延长本轮
                // 可见性目标的完成时间.
                FreezeResult freeze = executeFreeze("PAIMON_VISIBILITY_LAG", state);
                pendingPaimonFenceSequenceId = freeze.fenceSequenceId();
                requestFlush();
                continue;
            }

            if (state.newSSTCount() > config.newSstMaxCount()) {
                // Prefer reducing local read amplification. If no adjacent NEW runs fit within the
                // compaction byte limit, advance the oldest stable prefix to Paimon instead.
                // 优先通过 compact 降低本地点查放大; 如果没有相邻 NEW run 能满足 compact 字节上限,
                // 则改为将最老的稳定前缀推进到 Paimon.
                Optional<CompactionSelection> selection = selectCompaction(state, SSTState.NEW);
                if (selection.isPresent()) {
                    CompactionResult result = executeCompaction(state, selection.get(), "NEW_SST_COUNT");
                    if (!result.progressed()) {
                        return;
                    }
                } else {
                    SinkOperationResult result = executeSink(
                        state,
                        state.newSSTMaxSequenceId(),
                        "NEW_SST_COUNT"
                    );
                    if (!result.progressed()) {
                        return;
                    }
                }
                continue;
            }

            if (state.sinkedSSTCount() > config.sinkedSstMaxCount()) {
                // SINKED runs are safe to retire, but first compact any eligible adjacent group to
                // reduce local run count; otherwise fall back to oldest-first eviction.
                // SINKED run 可以安全退役, 但应先 compact 符合条件的相邻分组以减少本地 run 数量;
                // 如果不存在可 compact 分组, 再回退到从最老 run 开始淘汰.
                Optional<CompactionSelection> selection = selectCompaction(state, SSTState.SINKED);
                if (selection.isPresent()) {
                    CompactionResult result = executeCompaction(state, selection.get(), "SINKED_SST_COUNT");
                    if (!result.progressed()) {
                        return;
                    }
                } else {
                    EvictionResult result = executeEviction(state, "SINKED_SST_COUNT");
                    if (!result.progressed()) {
                        return;
                    }
                }
                continue;
            }
            return;
        }
        if (background && running) {
            requestMaintenance();
        }
    }

    private void drainSinkTarget(String reason, long targetSequenceId) {
        // Unlike automatic reconciliation, a synchronous command owns a fixed target and is not
        // action-budgeted. Monotonic persisted progress bounds the loop; NOOP is an explicit error.
        // 与自动调和不同, 同步命令持有固定目标, 不受单轮动作预算限制. persisted boundary 的单调
        // 推进保证循环有界; 如果 Operation 返回 NOOP, 则将其作为明确错误处理.
        while (targetSequenceId > 0) {
            BucketStateSnapshot state = operations.stateSnapshot();
            if (state.lastPersistedSequenceId() >= targetSequenceId) {
                return;
            }
            if (state.sinkFlight().status() == SinkFlightSnapshot.Status.PREPARED_RETRY) {
                if (!executePreparedRetry(state).progressed()) {
                    throw new IllegalStateException(
                        "prepared Sink made no progress toward target " + targetSequenceId
                            + ", lastPersisted=" + state.lastPersistedSequenceId()
                    );
                }
                continue;
            }
            SinkOperationResult result = executeSink(state, targetSequenceId, reason);
            if (!result.progressed()) {
                throw new IllegalStateException(
                    "Sink made no progress toward target " + targetSequenceId
                        + ", lastPersisted=" + state.lastPersistedSequenceId()
                );
            }
        }
    }

    private FreezeResult executeFreeze(String reason, BucketStateSnapshot state) {
        lastMaintenanceAction = "FREEZE";
        lastMaintenanceReason = reason;
        return executeAction(
            "maintenance",
            "FREEZE",
            reason,
            "lastAssignedSequenceId=" + state.lastAssignedSequenceId(),
            operations::freezeCurMemTable,
            FreezeResult::progressed
        );
    }

    private SinkOperationResult executePreparedRetry(BucketStateSnapshot state) {
        lastMaintenanceAction = "COMMIT_PREPARED_SINK";
        lastMaintenanceReason = "PREPARED_RETRY";
        return executeAction(
            "maintenance",
            "COMMIT_PREPARED_SINK",
            "PREPARED_RETRY",
            "batchId=" + state.sinkFlight().batchId()
                + " fenceFlushId=" + state.sinkFlight().sinkFenceFlushId(),
            operations::commitPreparedSink,
            SinkOperationResult::progressed
        );
    }

    private SinkOperationResult executeSink(BucketStateSnapshot state, long targetSequenceId, String reason) {
        if (targetSequenceId <= 0) {
            return SinkOperationResult.noop();
        }
        SinkSelection selection = new SinkSelection(
            targetSequenceId,
            config.sinkBatchMaxSsts(),
            config.sinkBatchMaxBytes()
        );
        lastMaintenanceAction = "SINK";
        lastMaintenanceReason = reason;
        return executeAction(
            "maintenance",
            "SINK",
            reason,
            "targetSequenceId=" + targetSequenceId
                + " newSstCount=" + state.newSSTCount()
                + " batchMaxSsts=" + config.sinkBatchMaxSsts()
                + " batchMaxBytes=" + config.sinkBatchMaxBytes(),
            () -> operations.sinkToPaimon(selection),
            SinkOperationResult::progressed
        );
    }

    private CompactionResult executeCompaction(
            BucketStateSnapshot state,
            CompactionSelection selection,
            String reason) {
        lastMaintenanceAction = "COMPACT_" + selection.state().name();
        lastMaintenanceReason = reason;
        return executeAction(
            "maintenance",
            lastMaintenanceAction,
            reason,
            "runIds=" + selection.inputRunIds()
                + " newSstCount=" + state.newSSTCount()
                + " sinkedSstCount=" + state.sinkedSSTCount(),
            () -> operations.compactLocalSSTs(selection),
            CompactionResult::progressed
        );
    }

    private EvictionResult executeEviction(BucketStateSnapshot state, String reason) {
        lastMaintenanceAction = "EVICT_OLDEST_SINKED";
        lastMaintenanceReason = reason;
        return executeAction(
            "maintenance",
            "EVICT_OLDEST_SINKED",
            reason,
            "sinkedSstCount=" + state.sinkedSSTCount(),
            operations::evictOldestSinkedSST,
            EvictionResult::progressed
        );
    }

    private Optional<CompactionSelection> selectCompaction(BucketStateSnapshot state, SSTState targetState) {
        // localRuns are ordered oldest-first by flush range. Greedily choose the first adjacent
        // group of at least two runs whose total input fits the configured byte ceiling.
        // localRuns 按 flush range 从老到新排列.使用贪心策略选择第一个相邻分组: 至少包含两个
        // run, 并且输入总大小不超过配置的字节上限.
        List<LocalRunSnapshot> runs = state.localRuns().stream()
            .filter(run -> run.state() == targetState)
            .toList();
        long maxInputBytes = config.compactMaxInputBytes();
        for (int start = 0; start + 1 < runs.size(); start++) {
            List<Long> selected = new ArrayList<>();
            LocalRunSnapshot first = runs.get(start);
            if (first.fileSizeBytes() > maxInputBytes) {
                continue;
            }
            selected.add(first.runId());
            long selectedBytes = first.fileSizeBytes();
            long previousMaxFlushId = first.maxFlushId();
            for (int index = start + 1; index < runs.size(); index++) {
                LocalRunSnapshot run = runs.get(index);
                if (previousMaxFlushId + 1 != run.minFlushId()
                        || run.fileSizeBytes() > maxInputBytes - selectedBytes) {
                    break;
                }
                selected.add(run.runId());
                selectedBytes += run.fileSizeBytes();
                previousMaxFlushId = run.maxFlushId();
            }
            if (selected.size() >= 2) {
                return Optional.of(new CompactionSelection(targetState, selected));
            }
        }
        return Optional.empty();
    }

    private boolean visibilityDue(BucketStateSnapshot state) {
        // Recovery must not wait for possibly stale/missing wall-clock ages: any recovered data not
        // known to be in Paimon establishes a new visibility fence immediately.
        // 恢复流程不能等待可能已失真或缺失的 wall-clock age; 只要恢复出的数据尚不能确认已进入
        // Paimon, 就应立即建立新的可见性 fence.
        if (state.recoveredUnpersistedData()) {
            return true;
        }
        long oldestWriteAtMillis = oldestPositive(
            state.curMemTableOldestWriteAtMillis(),
            state.immutableMemTableOldestWriteAtMillis(),
            state.newSSTOldestWriteAtMillis()
        );
        return oldestWriteAtMillis > 0
            && nowMillis() - oldestWriteAtMillis >= config.visibilityMaxDelayMs();
    }

    private static long oldestPositive(long... values) {
        long oldest = 0;
        for (long value : values) {
            if (value > 0 && (oldest == 0 || value < oldest)) {
                oldest = value;
            }
        }
        return oldest;
    }

    private <T> T executeAction(
            String worker,
            String action,
            String reason,
            String context,
            Supplier<T> operation,
            java.util.function.Predicate<T> progressed) {
        LOG.info(
            "{} phase=start worker={} action={} reason={} {}",
            ACTION_LOG_MARKER,
            worker,
            action,
            reason,
            context
        );
        try {
            T result = operation.get();
            boolean madeProgress = progressed.test(result);
            increment(madeProgress ? actionSuccessCounts : actionNoopCounts, action);
            LOG.info(
                "{} phase={} worker={} action={} reason={} {}",
                ACTION_LOG_MARKER,
                madeProgress ? "completed" : "noop",
                worker,
                action,
                reason,
                context
            );
            return result;
        } catch (RuntimeException e) {
            increment(actionFailureCounts, action);
            lastErrorAt = now();
            lastErrorWorker = worker;
            lastErrorAction = action;
            lastErrorMessage = e.getMessage();
            LOG.error(
                "{} phase=failed worker={} action={} reason={} {}",
                ACTION_LOG_MARKER,
                worker,
                action,
                reason,
                context,
                e
            );
            throw e;
        }
    }

    private void recordWorkerFailure(String worker, String action, RuntimeException failure) {
        lastErrorAt = now();
        lastErrorWorker = worker;
        lastErrorAction = action;
        lastErrorMessage = failure.getMessage();
    }

    private long retryNotBefore() {
        return Math.addExact(nowMillis(), config.failureRetryDelayMs());
    }

    private void scheduleFlushRetry() {
        if (running && !closed) {
            flushExecutor.schedule(this::requestFlush, config.failureRetryDelayMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleMaintenanceRetry() {
        if (running && !closed) {
            maintenanceExecutor.schedule(
                this::requestMaintenance,
                config.failureRetryDelayMs(),
                TimeUnit.MILLISECONDS
            );
        }
    }

    private <T> T callFlush(Callable<T> action) {
        return await(flushExecutor.submit(action));
    }

    private <T> T callMaintenance(Callable<T> action) {
        return await(maintenanceExecutor.submit(action));
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for PMS scheduler operation", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("PMS scheduler operation failed", cause);
        }
    }

    private static void increment(ConcurrentHashMap<String, AtomicLong> counts, String action) {
        counts.computeIfAbsent(action, ignored -> new AtomicLong()).incrementAndGet();
    }

    private static Map<String, Long> countSnapshot(ConcurrentHashMap<String, AtomicLong> counts) {
        Map<String, Long> snapshot = new TreeMap<>();
        counts.forEach((action, count) -> snapshot.put(action, count.get()));
        return snapshot;
    }

    private static void cancel(Future<?> task) {
        if (task != null) {
            task.cancel(false);
        }
    }

    private static void shutdown(ScheduledExecutorService executor, String role) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("PMS {} scheduler worker did not stop within timeout; forcing shutdown", role);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PMS server scheduler is closed");
        }
    }

    private String now() {
        return timeSource.wallClockNow().toString();
    }

    private long nowMillis() {
        return timeSource.wallClockNow().toEpochMilli();
    }

    private long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(timeSource.monotonicNanos() - startedNanos);
    }

    interface Operations {
        BucketStateSnapshot stateSnapshot();

        FreezeResult freezeCurMemTable();

        FlushResult flushImmutableMemTable();

        SinkOperationResult sinkToPaimon(SinkSelection selection);

        SinkOperationResult commitPreparedSink();

        CompactionResult compactLocalSSTs(CompactionSelection selection);

        EvictionResult evictOldestSinkedSST();
    }
}
