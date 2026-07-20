package org.qwh.pms.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.qwh.pms.core.bucket.BucketStateSnapshot;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.bucket.SinkFlightSnapshot;
import org.qwh.pms.core.bucket.operation.CompactionResult;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.EvictionResult;
import org.qwh.pms.core.bucket.operation.FlushResult;
import org.qwh.pms.core.bucket.operation.FreezeResult;
import org.qwh.pms.core.bucket.operation.OperationStatus;
import org.qwh.pms.core.bucket.operation.SinkOperationResult;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.storage.SSTState;

class PmsServerSchedulerTest {

    @Test
    void configKeepsFlushAndMaintenanceIntervalsIndependent() {
        PmsSchedulerConfig config = config(true, 1_000, 30_000, 600_000, 10, 10, 4, 1_024, 1_024);

        assertEquals(1_000, config.flushReconcileIntervalMs());
        assertEquals(30_000, config.maintenanceReconcileIntervalMs());
        assertEquals(1_024L * 1024 * 1024, config.sinkBatchMaxBytes());
        assertEquals(1_024L * 1024 * 1024, config.compactMaxInputBytes());
        assertThrows(
            IllegalArgumentException.class,
            () -> config(true, 0, 30_000, 600_000, 10, 10, 4, 1_024, 1_024)
        );
    }

    @Test
    void productionDefaultsEnableTheTwoCadences() {
        PmsSchedulerConfig config = PmsSchedulerConfig.defaults();

        assertTrue(config.enabled());
        assertEquals(1_000, config.flushReconcileIntervalMs());
        assertEquals(30_000, config.maintenanceReconcileIntervalMs());
        assertEquals(600_000, config.visibilityMaxDelayMs());
    }

    @Test
    void flushWorkerDrainsImmutableBacklogWithoutFreezing() throws Exception {
        MutableOperations operations = new MutableOperations();
        operations.lastAssignedSequenceId = 3;
        operations.immutableCount = 3;
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(true, 1, 60_000, 600_000, 10, 10, 4, 1_024, 1_024),
            systemLikeTimeSource()
        );
        try {
            scheduler.start();
            waitUntil(() -> operations.immutableCount == 0);
        } finally {
            scheduler.close();
        }

        assertEquals(3, operations.flushCalls);
        assertEquals(0, operations.freezeCalls);
        assertTrue((Long) scheduler.state().get("flushRunCount") > 0);
    }

    @Test
    void visibilityFenceFreezesFlushesAndSinksThroughOneController() {
        MutableOperations operations = new MutableOperations();
        operations.currentEntries = 1;
        operations.lastAssignedSequenceId = 5;
        operations.currentOldestWriteAtMillis = 1_000;
        SchedulerTimeSource timeSource = fixedTimeSource(Instant.ofEpochMilli(2_000));
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 1_000, 30_000, 500, 10, 10, 1, 1_024, 1_024),
            timeSource
        );
        try {
            scheduler.reconcileNow();
        } finally {
            scheduler.close();
        }

        assertEquals(1, operations.freezeCalls);
        assertEquals(1, operations.flushCalls);
        assertEquals(1, operations.sinkCalls);
        assertEquals(5, operations.lastFlushedSequenceId);
        assertEquals(5, operations.lastPersistedSequenceId);
        assertEquals(0L, scheduler.state().get("pendingPaimonFenceSequenceId"));
    }

    @Test
    void visibilityFenceAdvancesThroughMultipleBoundedSinkBatches() {
        MutableOperations operations = new MutableOperations();
        operations.addRun(SSTState.NEW, 1, 100);
        operations.addRun(SSTState.NEW, 2, 100);
        operations.addRun(SSTState.NEW, 3, 100);
        operations.lastFlushedSequenceId = 3;
        operations.recoveredUnpersistedData = true;
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 1_000, 30_000, 600_000, 10, 10, 1, 1_024, 1_024),
            fixedTimeSource(Instant.ofEpochMilli(10_000))
        );
        try {
            scheduler.reconcileNow();
        } finally {
            scheduler.close();
        }

        assertEquals(3, operations.sinkCalls);
        assertEquals(3, operations.lastPersistedSequenceId);
        assertEquals(0, operations.count(SSTState.NEW));
        assertEquals(3, operations.count(SSTState.SINKED));
        assertEquals(0L, scheduler.state().get("pendingPaimonFenceSequenceId"));
    }

    @Test
    void preparedSinkRetryRunsBeforeOrdinaryMaintenance() {
        MutableOperations operations = new MutableOperations();
        LocalRunSnapshot preparedRun = operations.addRun(SSTState.NEW, 1, 100);
        operations.lastFlushedSequenceId = preparedRun.maxSequenceId();
        operations.sinkFlight = new SinkFlightSnapshot(
            SinkFlightSnapshot.Status.PREPARED_RETRY,
            "prepared-1",
            preparedRun.maxFlushId(),
            preparedRun.minSequenceId(),
            preparedRun.maxSequenceId()
        );
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 1_000, 30_000, 600_000, 10, 10, 4, 1_024, 1_024),
            fixedTimeSource(Instant.ofEpochMilli(10_000))
        );
        try {
            scheduler.reconcileNow();
        } finally {
            scheduler.close();
        }

        assertEquals(1, operations.preparedCommitCalls);
        assertEquals(0, operations.sinkCalls);
        assertEquals(SinkFlightSnapshot.Status.IDLE, operations.sinkFlight.status());
        assertEquals(0, operations.count(SSTState.NEW));
        assertEquals(1, operations.count(SSTState.SINKED));
    }

    @Test
    void manualFlushReturnsFenceBeforeBlockedFlushCompletes() throws Exception {
        MutableOperations operations = new MutableOperations();
        operations.currentEntries = 1;
        operations.lastAssignedSequenceId = 5;
        operations.blockNextFlush();
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 60_000, 60_000, 600_000, 10, 10, 1, 1_024, 1_024),
            systemLikeTimeSource()
        );
        try {
            scheduler.start();
            long fence = scheduler.requestFlushToCurrent();

            assertEquals(5, fence);
            assertTrue(operations.flushStarted.await(5, TimeUnit.SECONDS));
            assertEquals(0, operations.lastFlushedSequenceId);
            operations.releaseFlush();
            waitUntil(() -> operations.lastFlushedSequenceId >= fence);
        } finally {
            operations.releaseFlush();
            scheduler.close();
        }

        assertEquals(1, operations.freezeCalls);
        assertEquals(1, operations.flushCalls);
        assertEquals(5, operations.lastFlushedSequenceId);
        assertEquals(0, operations.lastPersistedSequenceId);
    }

    @Test
    void closeDoesNotWaitForLongDelayedRetry() throws Exception {
        MutableOperations operations = new MutableOperations();
        operations.addImmutable(1);
        operations.flushFailure = new RuntimeException("injected retryable Flush failure");
        PmsSchedulerConfig config = new PmsSchedulerConfig(
            true,
            1,
            60_000,
            60_000,
            600_000,
            10,
            10,
            4,
            1_024,
            1_024
        );
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config,
            systemLikeTimeSource()
        );
        try {
            scheduler.start();
            waitUntil(() -> operations.flushCalls > 0);
            assertTimeoutPreemptively(Duration.ofSeconds(2), scheduler::close);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void manualSinkReusesVisibilityFenceAndCompletesAsynchronously() throws Exception {
        MutableOperations operations = new MutableOperations();
        operations.currentEntries = 1;
        operations.lastAssignedSequenceId = 3;
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(true, 60_000, 60_000, 600_000, 10, 10, 1, 1_024, 1_024),
            systemLikeTimeSource()
        );
        try {
            scheduler.start();
            long fence = scheduler.requestSinkToCurrent();

            assertEquals(3, fence);
            waitUntil(() -> operations.lastPersistedSequenceId >= fence
                && (Long) scheduler.state().get("pendingPaimonFenceSequenceId") == 0L);
        } finally {
            scheduler.close();
        }

        assertEquals(1, operations.freezeCalls);
        assertEquals(1, operations.flushCalls);
        assertEquals(1, operations.sinkCalls);
        assertEquals(3, operations.lastFlushedSequenceId);
        assertEquals(3, operations.lastPersistedSequenceId);
        assertEquals(0L, scheduler.state().get("pendingPaimonFenceSequenceId"));
    }

    @Test
    void newCountCompactsOneContinuousGroupBeforeConsideringSink() {
        MutableOperations operations = new MutableOperations();
        operations.addRun(SSTState.NEW, 1, 100);
        operations.addRun(SSTState.NEW, 2, 100);
        operations.addRun(SSTState.NEW, 3, 100);
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 1_000, 30_000, 600_000, 2, 10, 4, 1_024, 1_024),
            fixedTimeSource(Instant.ofEpochMilli(10_000))
        );
        try {
            scheduler.reconcileNow();
        } finally {
            scheduler.close();
        }

        assertEquals(1, operations.compactCalls);
        assertEquals(0, operations.sinkCalls);
        assertEquals(1, operations.count(SSTState.NEW));
    }

    @Test
    void sinkedCountEvictsOldestWhenNoPairFitsCompactLimit() {
        MutableOperations operations = new MutableOperations();
        operations.addRun(SSTState.SINKED, 1, 700L * 1024 * 1024);
        operations.addRun(SSTState.SINKED, 2, 700L * 1024 * 1024);
        PmsServerScheduler scheduler = new PmsServerScheduler(
            operations,
            config(false, 1_000, 30_000, 600_000, 10, 1, 4, 1_024, 1_024),
            fixedTimeSource(Instant.ofEpochMilli(10_000))
        );
        try {
            scheduler.reconcileNow();
        } finally {
            scheduler.close();
        }

        assertEquals(0, operations.compactCalls);
        assertEquals(1, operations.evictCalls);
        assertEquals(List.of(2L), operations.runIds(SSTState.SINKED));
    }

    @Test
    void operationLogMarkerIsStableAndGrepFriendly() {
        assertEquals("PMS_SCHEDULER_ACTION", PmsServerScheduler.ACTION_LOG_MARKER);
    }

    private static PmsSchedulerConfig config(
            boolean enabled,
            int flushIntervalMs,
            int maintenanceIntervalMs,
            long visibilityMaxDelayMs,
            int newMaxCount,
            int sinkedMaxCount,
            int sinkBatchMaxSsts,
            int sinkBatchMaxBytesMb,
            int compactMaxInputSizeMb) {
        return new PmsSchedulerConfig(
            enabled,
            flushIntervalMs,
            maintenanceIntervalMs,
            5_000,
            visibilityMaxDelayMs,
            newMaxCount,
            sinkedMaxCount,
            sinkBatchMaxSsts,
            sinkBatchMaxBytesMb,
            compactMaxInputSizeMb
        );
    }

    private static SchedulerTimeSource systemLikeTimeSource() {
        return new SchedulerTimeSource() {
            @Override
            public Instant wallClockNow() {
                return Instant.now();
            }

            @Override
            public long monotonicNanos() {
                return System.nanoTime();
            }
        };
    }

    private static SchedulerTimeSource fixedTimeSource(Instant instant) {
        AtomicLong monotonicNanos = new AtomicLong();
        return new SchedulerTimeSource() {
            @Override
            public Instant wallClockNow() {
                return instant;
            }

            @Override
            public long monotonicNanos() {
                return monotonicNanos.getAndAdd(TimeUnit.MILLISECONDS.toNanos(1));
            }
        };
    }

    private static void waitUntil(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true before timeout");
    }

    private static final class MutableOperations implements PmsServerScheduler.Operations {
        private int currentEntries;
        private int immutableCount;
        private long lastAssignedSequenceId;
        private long lastFlushedSequenceId;
        private long lastPersistedSequenceId;
        private long currentOldestWriteAtMillis;
        private long nextRunId = 1;
        private long nextFlushId = 1;
        private int freezeCalls;
        private int flushCalls;
        private int sinkCalls;
        private int preparedCommitCalls;
        private int compactCalls;
        private int evictCalls;
        private RuntimeException flushFailure;
        private volatile CountDownLatch flushStarted;
        private volatile CountDownLatch flushRelease;
        private boolean recoveredUnpersistedData;
        private SinkFlightSnapshot sinkFlight = SinkFlightSnapshot.idle();
        private final ArrayDeque<Long> immutableMaxSequenceIds = new ArrayDeque<>();
        private final List<LocalRunSnapshot> runs = new ArrayList<>();

        @Override
        public synchronized BucketStateSnapshot stateSnapshot() {
            List<LocalRunSnapshot> newRuns = runs(SSTState.NEW);
            List<LocalRunSnapshot> sinkedRuns = runs(SSTState.SINKED);
            return new BucketStateSnapshot(
                10_000,
                currentEntries,
                currentEntries * 100L,
                currentEntries == 0 ? 0 : lastAssignedSequenceId,
                currentEntries == 0 ? 0 : lastAssignedSequenceId,
                currentOldestWriteAtMillis,
                0,
                immutableCount,
                immutableCount * 100L,
                immutableCount == 0 ? 0 : 1,
                immutableCount == 0 ? 0 : lastAssignedSequenceId,
                0,
                0,
                lastAssignedSequenceId,
                lastFlushedSequenceId,
                lastPersistedSequenceId,
                newRuns.size(),
                totalBytes(newRuns),
                newRuns.size(),
                minSequence(newRuns),
                maxSequence(newRuns),
                0,
                0,
                sinkedRuns.size(),
                totalBytes(sinkedRuns),
                sinkedRuns.size(),
                minSequence(sinkedRuns),
                maxSequence(sinkedRuns),
                0,
                0,
                List.copyOf(runs),
                sinkFlight,
                recoveredUnpersistedData && lastPersistedSequenceId < lastAssignedSequenceId,
                lastPersistedSequenceId == 0 ? 0 : 1
            );
        }

        @Override
        public synchronized FreezeResult freezeCurMemTable() {
            freezeCalls++;
            if (currentEntries == 0) {
                return FreezeResult.noop(lastAssignedSequenceId);
            }
            int entries = currentEntries;
            currentEntries = 0;
            immutableCount++;
            immutableMaxSequenceIds.addLast(lastAssignedSequenceId);
            currentOldestWriteAtMillis = 0;
            return new FreezeResult(
                OperationStatus.PROGRESSED,
                lastAssignedSequenceId,
                lastAssignedSequenceId,
                lastAssignedSequenceId,
                1_000,
                entries,
                entries * 100L
            );
        }

        @Override
        public synchronized FlushResult flushImmutableMemTable() {
            if (immutableCount == 0) {
                return FlushResult.noop();
            }
            flushCalls++;
            if (flushFailure != null) {
                throw flushFailure;
            }
            if (flushStarted != null) {
                flushStarted.countDown();
                try {
                    flushRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("blocked Flush interrupted", e);
                }
                flushStarted = null;
                flushRelease = null;
            }
            immutableCount--;
            long flushedSequenceId = immutableMaxSequenceIds.isEmpty()
                ? lastAssignedSequenceId
                : immutableMaxSequenceIds.removeFirst();
            lastFlushedSequenceId = Math.max(lastFlushedSequenceId, flushedSequenceId);
            LocalRunSnapshot run = addRun(SSTState.NEW, nextFlushId++, 100, flushedSequenceId);
            return new FlushResult(OperationStatus.PROGRESSED, Optional.of(run));
        }

        @Override
        public synchronized SinkOperationResult sinkToPaimon(SinkSelection selection) {
            List<LocalRunSnapshot> selected = new ArrayList<>();
            long selectedBytes = 0;
            for (LocalRunSnapshot run : runs(SSTState.NEW)) {
                if (run.maxSequenceId() > selection.targetSequenceId()
                        || selected.size() >= selection.maxSstCount()
                        || (!selected.isEmpty() && run.fileSizeBytes() > selection.maxInputBytes() - selectedBytes)) {
                    break;
                }
                selected.add(run);
                selectedBytes += run.fileSizeBytes();
            }
            if (selected.isEmpty()) {
                return SinkOperationResult.noop();
            }
            sinkCalls++;
            runs.removeAll(selected);
            List<LocalRunSnapshot> sinked = selected.stream().map(run -> withState(run, SSTState.SINKED)).toList();
            runs.addAll(sinked);
            sortRuns();
            lastPersistedSequenceId = selected.get(selected.size() - 1).maxSequenceId();
            SinkCommitResult commit = new SinkCommitResult(
                "batch-" + sinkCalls,
                sinkCalls,
                lastPersistedSequenceId,
                selected.stream().map(LocalRunSnapshot::runId).toList()
            );
            return new SinkOperationResult(OperationStatus.PROGRESSED, Optional.of(commit), sinked);
        }

        @Override
        public synchronized SinkOperationResult commitPreparedSink() {
            if (sinkFlight.status() != SinkFlightSnapshot.Status.PREPARED_RETRY) {
                return SinkOperationResult.noop();
            }
            List<LocalRunSnapshot> prepared = runs(SSTState.NEW).stream()
                .filter(run -> run.maxSequenceId() <= sinkFlight.maxSequenceId())
                .toList();
            if (prepared.isEmpty()) {
                throw new IllegalStateException("prepared Sink has no matching NEW runs");
            }
            preparedCommitCalls++;
            runs.removeAll(prepared);
            List<LocalRunSnapshot> sinked = prepared.stream()
                .map(run -> withState(run, SSTState.SINKED))
                .toList();
            runs.addAll(sinked);
            sortRuns();
            lastPersistedSequenceId = sinkFlight.maxSequenceId();
            SinkCommitResult commit = new SinkCommitResult(
                sinkFlight.batchId(),
                preparedCommitCalls,
                lastPersistedSequenceId,
                prepared.stream().map(LocalRunSnapshot::runId).toList()
            );
            sinkFlight = SinkFlightSnapshot.idle();
            return new SinkOperationResult(OperationStatus.PROGRESSED, Optional.of(commit), sinked);
        }

        @Override
        public synchronized CompactionResult compactLocalSSTs(CompactionSelection selection) {
            List<LocalRunSnapshot> selected = runs.stream()
                .filter(run -> selection.inputRunIds().contains(run.runId()))
                .toList();
            if (selected.size() != selection.inputRunIds().size()) {
                return CompactionResult.noop();
            }
            compactCalls++;
            runs.removeAll(selected);
            LocalRunSnapshot output = new LocalRunSnapshot(
                nextRunId++,
                selected.get(0).minFlushId(),
                selected.get(selected.size() - 1).maxFlushId(),
                selection.state(),
                totalBytes(selected),
                selected.size(),
                minSequence(selected),
                maxSequence(selected),
                0,
                0
            );
            runs.add(output);
            sortRuns();
            return new CompactionResult(
                OperationStatus.PROGRESSED,
                Optional.of(new CompactionResult.Group(selection.state(), selection.inputRunIds(), output))
            );
        }

        @Override
        public synchronized EvictionResult evictOldestSinkedSST() {
            Optional<LocalRunSnapshot> oldest = runs(SSTState.SINKED).stream().findFirst();
            if (oldest.isEmpty()) {
                return EvictionResult.noop();
            }
            evictCalls++;
            runs.remove(oldest.get());
            return new EvictionResult(OperationStatus.PROGRESSED, oldest);
        }

        private synchronized LocalRunSnapshot addRun(SSTState state, long flushId, long bytes) {
            long sequenceId = Math.max(lastAssignedSequenceId, flushId);
            return addRun(state, flushId, bytes, sequenceId);
        }

        private synchronized LocalRunSnapshot addRun(
                SSTState state,
                long flushId,
                long bytes,
                long sequenceId) {
            lastAssignedSequenceId = Math.max(lastAssignedSequenceId, sequenceId);
            nextFlushId = Math.max(nextFlushId, flushId + 1);
            LocalRunSnapshot run = new LocalRunSnapshot(
                nextRunId++,
                flushId,
                flushId,
                state,
                bytes,
                1,
                sequenceId,
                sequenceId,
                0,
                0
            );
            runs.add(run);
            sortRuns();
            return run;
        }

        private synchronized void addImmutable(long maxSequenceId) {
            immutableCount++;
            immutableMaxSequenceIds.addLast(maxSequenceId);
            lastAssignedSequenceId = Math.max(lastAssignedSequenceId, maxSequenceId);
        }

        private synchronized void blockNextFlush() {
            flushStarted = new CountDownLatch(1);
            flushRelease = new CountDownLatch(1);
        }

        private void releaseFlush() {
            CountDownLatch release = flushRelease;
            if (release != null) {
                release.countDown();
            }
        }

        private synchronized int count(SSTState state) {
            return runs(state).size();
        }

        private synchronized List<Long> runIds(SSTState state) {
            return runs(state).stream().map(LocalRunSnapshot::runId).toList();
        }

        private List<LocalRunSnapshot> runs(SSTState state) {
            return runs.stream().filter(run -> run.state() == state).toList();
        }

        private void sortRuns() {
            runs.sort(Comparator.comparingLong(LocalRunSnapshot::minFlushId));
        }

        private static LocalRunSnapshot withState(LocalRunSnapshot run, SSTState state) {
            return new LocalRunSnapshot(
                run.runId(),
                run.minFlushId(),
                run.maxFlushId(),
                state,
                run.fileSizeBytes(),
                run.entryCount(),
                run.minSequenceId(),
                run.maxSequenceId(),
                run.oldestWriteAtMillis(),
                run.ageMillis()
            );
        }

        private static long totalBytes(List<LocalRunSnapshot> runs) {
            return runs.stream().mapToLong(LocalRunSnapshot::fileSizeBytes).sum();
        }

        private static long minSequence(List<LocalRunSnapshot> runs) {
            return runs.stream().mapToLong(LocalRunSnapshot::minSequenceId).min().orElse(0);
        }

        private static long maxSequence(List<LocalRunSnapshot> runs) {
            return runs.stream().mapToLong(LocalRunSnapshot::maxSequenceId).max().orElse(0);
        }
    }
}
