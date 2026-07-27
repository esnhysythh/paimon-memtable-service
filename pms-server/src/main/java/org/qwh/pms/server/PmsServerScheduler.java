package org.qwh.pms.server;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
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
 * Coordinates background maintenance and asynchronous fence requests for one PMS table.
 * 为单个 PMS 表协调后台维护和异步 fence 请求.
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
 * requests establish a sequence fence and return without waiting for Flush or Sink I/O; the same
 * workers advance that fence and expose completion through the durable sequence boundaries.
 *
 * <p>自动调和按照 {@link #MAX_ACTIONS_PER_RUN} 划分执行片段, 以保证调度公平性.管理请求只建立
 * sequence fence, 不等待 Flush 或 Sink I/O; 同一组 worker 负责推进 fence, 客户端通过可靠的
 * sequence boundary 观察完成状态.
 */
public final class PmsServerScheduler implements AutoCloseable {
    static final String ACTION_LOG_MARKER = "PMS_SCHEDULER_ACTION";
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerScheduler.class);
    private static final AtomicInteger THREAD_ID = new AtomicInteger();
    private static final int MAX_ACTIONS_PER_RUN = 64;

    private final Operations operations;
    private final PmsSchedulerConfig config;
    private final Clock clock;
    private final ScheduledExecutorService flushExecutor;
    private final ScheduledExecutorService maintenanceExecutor;
    private final AtomicBoolean flushQueuedOrRunning = new AtomicBoolean();
    private final AtomicBoolean maintenanceQueuedOrRunning = new AtomicBoolean();
    private final AtomicBoolean flushRerunRequested = new AtomicBoolean();
    private final AtomicBoolean maintenanceRerunRequested = new AtomicBoolean();

    private volatile boolean running;
    private volatile boolean closed;
    private volatile boolean flushRunning;
    private volatile boolean maintenanceRunning;
    // The only cross-operation plan state. Zero means no visibility fence is active.
    // 唯一跨 Operation 保留的计划状态; 零表示当前没有生效的可见性 fence.
    private final AtomicLong pendingPaimonFenceSequenceId = new AtomicLong();

    public PmsServerScheduler(PmsTableService service, PmsSchedulerConfig config) {
        this(requireService(service), config, Clock.systemUTC());
    }

    PmsServerScheduler(Operations operations, PmsSchedulerConfig config, Clock clock) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.config = config == null ? PmsSchedulerConfig.defaults() : config;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
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
        running = true;
        flushExecutor.scheduleWithFixedDelay(
            this::signalFlush,
            0,
            config.flushReconcileIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        maintenanceExecutor.scheduleWithFixedDelay(
            this::signalMaintenance,
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

    /** Establishes a local Flush fence and returns without waiting for SST I/O. */
    public synchronized long requestFlushToCurrent() {
        ensureRunning();
        FreezeResult freeze = executeFreeze("MANUAL_FLUSH_REQUEST", operations.stateSnapshot());
        signalFlush();
        return freeze.fenceSequenceId();
    }

    /** Establishes or extends the Paimon visibility fence and returns without waiting for I/O. */
    public synchronized long requestSinkToCurrent() {
        ensureRunning();
        FreezeResult freeze = executeFreeze("MANUAL_SINK_REQUEST", operations.stateSnapshot());
        long fence = freeze.fenceSequenceId();
        pendingPaimonFenceSequenceId.accumulateAndGet(fence, Math::max);
        signalFlush();
        signalMaintenance();
        return fence;
    }

    public Map<String, Object> state() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("running", running);
        result.put("flushReconcileIntervalMs", config.flushReconcileIntervalMs());
        result.put("maintenanceReconcileIntervalMs", config.maintenanceReconcileIntervalMs());
        result.put("visibilityMaxDelayMs", config.visibilityMaxDelayMs());
        result.put("newSstMaxCount", config.newSstMaxCount());
        result.put("sinkedSstMaxCount", config.sinkedSstMaxCount());
        result.put("sinkBatchMaxBytes", config.sinkBatchMaxBytes());
        result.put("compactMaxInputBytes", config.compactMaxInputBytes());
        result.put("pendingPaimonFenceSequenceId", pendingPaimonFenceSequenceId.get());
        result.put("flushRunning", flushRunning);
        result.put("maintenanceRunning", maintenanceRunning);
        return result;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        running = false;
        closed = true;
        flushExecutor.shutdown();
        maintenanceExecutor.shutdown();
        boolean interrupted = awaitTermination(flushExecutor) | awaitTermination(maintenanceExecutor);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        LOG.info("PMS server scheduler stopped");
    }

    private void signalFlush() {
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
                    signalFlush();
                }
            }
        });
    }

    private void signalMaintenance() {
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
                    signalMaintenance();
                }
            }
        });
    }

    private void runFlushSafely() {
        if (!running || closed) {
            return;
        }
        flushRunning = true;
        try {
            reconcileFlush("IMMUTABLE_BACKLOG");
        } catch (RuntimeException e) {
            LOG.error("PMS Flush worker reconciliation failed; retrying on the next worker trigger", e);
        } finally {
            flushRunning = false;
        }
    }

    private void runMaintenanceSafely() {
        if (!running || closed) {
            return;
        }
        maintenanceRunning = true;
        try {
            reconcileMaintenance();
        } catch (RuntimeException e) {
            LOG.error("PMS maintenance worker reconciliation failed; retrying on the next worker trigger", e);
        } finally {
            maintenanceRunning = false;
        }
    }

    private void reconcileFlush(String reason) {
        for (int actions = 0; actions < MAX_ACTIONS_PER_RUN; actions++) {
            if (!running) {
                return;
            }
            BucketStateSnapshot state = operations.stateSnapshot();
            // stateSnapshot may wait behind an in-flight storage publication. Recheck after it
            // returns so shutdown does not normally admit a new action from a stale worker pass.
            // This is intentionally a best-effort lifecycle boundary rather than a new action
            // admission lock; an action racing with the check is treated as already in flight.
            if (!running) {
                return;
            }
            if (state.immutableMemTableCount() == 0) {
                return;
            }
            FlushResult result = executeFlush(state, reason);
            if (!result.progressed()) {
                return;
            }
            if (running) {
                signalMaintenance();
            }
        }
        if (running && operations.stateSnapshot().immutableMemTableCount() > 0) {
            signalFlush();
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

    private void reconcileMaintenance() {
        // This is a priority reconciliation loop, not a precomputed plan. Every progressed action
        // invalidates the old snapshot, so the loop rereads state and starts again at priority 1.
        // 这是按优先级执行的调和循环, 而不是预先计算好的计划. 每个取得进展的动作都会使旧快照
        // 失效, 因此循环必须重新读取状态, 并从最高优先级重新判断.
        for (int actions = 0; actions < MAX_ACTIONS_PER_RUN; actions++) {
            if (!running) {
                return;
            }
            BucketStateSnapshot state = operations.stateSnapshot();
            // See reconcileFlush(): keep the stop check close to action selection without adding
            // a scheduler-wide lock to every maintenance operation.
            if (!running) {
                return;
            }

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

            long fence = pendingPaimonFenceSequenceId.get();
            if (fence > 0) {
                // Persisted coverage satisfies the fence; discard controller state and reconsider
                // ordinary count maintenance from a new snapshot.
                // persisted boundary 覆盖 fence 后, 该目标即已满足;清除 controller 状态, 并基于
                // 新快照重新判断常规的数量维护.
                if (state.lastPersistedSequenceId() >= fence) {
                    pendingPaimonFenceSequenceId.compareAndSet(fence, 0);
                    continue;
                }
                // Sink cannot cover the fence until Flush has materialized its complete prefix.
                // 在 Flush 将 fence 对应的完整前缀物化为 SST 之前, Sink 无法覆盖该 fence.
                if (state.lastFlushedSequenceId() < fence) {
                    signalFlush();
                    return;
                }
                // One Sink call is deliberately bounded by input bytes. Keep the same fence
                // across calls until lastPersistedSequenceId reaches it.
                // 单次 Sink 调用有意受到输入字节数限制; 在 lastPersistedSequenceId 达到
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
                pendingPaimonFenceSequenceId.accumulateAndGet(freeze.fenceSequenceId(), Math::max);
                signalFlush();
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
        if (running) {
            signalMaintenance();
        }
    }

    private FreezeResult executeFreeze(String reason, BucketStateSnapshot state) {
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
            config.sinkBatchMaxBytes()
        );
        return executeAction(
            "maintenance",
            "SINK",
            reason,
            "targetSequenceId=" + targetSequenceId
                + " newSstCount=" + state.newSSTCount()
                + " batchMaxBytes=" + config.sinkBatchMaxBytes(),
            () -> operations.sinkToPaimon(selection),
            SinkOperationResult::progressed
        );
    }

    private CompactionResult executeCompaction(
            BucketStateSnapshot state,
            CompactionSelection selection,
            String reason) {
        String action = "COMPACT_" + selection.state().name();
        return executeAction(
            "maintenance",
            action,
            reason,
            "runIds=" + selection.inputRunIds()
                + " newSstCount=" + state.newSSTCount()
                + " sinkedSstCount=" + state.sinkedSSTCount(),
            () -> operations.compactLocalSSTs(selection),
            CompactionResult::progressed
        );
    }

    private EvictionResult executeEviction(BucketStateSnapshot state, String reason) {
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

    private static boolean awaitTermination(ScheduledExecutorService executor) {
        boolean interrupted = false;
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PMS server scheduler is closed");
        }
    }

    private void ensureRunning() {
        ensureOpen();
        if (!running) {
            throw new IllegalStateException("PMS server scheduler is not running");
        }
    }

    private long nowMillis() {
        return clock.millis();
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
