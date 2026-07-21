package org.qwh.pms.core.bucket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.bucket.PMSBucketDirector.WriteOp;
import org.qwh.pms.core.bucket.operation.CompactionResult;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.EvictionResult;
import org.qwh.pms.core.bucket.operation.FlushResult;
import org.qwh.pms.core.bucket.operation.FreezeResult;
import org.qwh.pms.core.bucket.operation.SinkOperationResult;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.config.*;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.sink.MockSinkManager;
import org.qwh.pms.core.storage.SSTMeta;
import org.qwh.pms.core.storage.SSTState;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PMSBucketDirectorImplTest {

    @TempDir
    Path tempDir;

    private PMSConfig config(int memtableMaxEntries, int memtableMaxSizeMb) {
        return config(memtableMaxEntries, memtableMaxSizeMb, 100, 32, 4);
    }

    private PMSConfig config(
            int memtableMaxEntries,
            int memtableMaxSizeMb,
            int sinkedMaxCount,
            int compactThresholdMb,
            int compactMinFiles) {
        return new PMSConfig(
            new MemTableConfig(memtableMaxEntries, memtableMaxSizeMb),
            new WalConfig(tempDir.resolve("wal").toString(), 256, false),
            new StorageConfig(tempDir.resolve("storage").toString(), 0, sinkedMaxCount, 0, compactThresholdMb, compactMinFiles),
            new SinkConfig(0, 0),
            new FlowControlConfig(Integer.MAX_VALUE, Integer.MAX_VALUE),
            new PaimonConfig("dummy", null)
        );
    }

    private PMSBucketDirectorImpl newDirector(PMSConfig config) {
        return newDirector(config, new MockSinkManager());
    }

    private PMSBucketDirectorImpl newDirector(PMSConfig config, SinkManager sinkManager) {
        return new PMSBucketDirectorImpl(config, storage -> sinkManager);
    }

    private static PMSConfig withFlowControl(
            PMSConfig config,
            int overloadedImmutableCount,
            int overloadedPendingSstCount) {
        return new PMSConfig(
            config.memtable(),
            config.wal(),
            config.storage(),
            config.sink(),
            new FlowControlConfig(overloadedImmutableCount, overloadedPendingSstCount),
            config.paimon()
        );
    }

    // ── Write path ──

    @Test
    void putAndGet() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            Optional<byte[]> result = dir.get("k1".getBytes());
            assertTrue(result.isPresent());
            assertArrayEquals("v1".getBytes(), result.get());
        } finally {
            dir.close();
        }
    }

    @Test
    void putSupportsEmptyValue() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), new byte[0]);

            Optional<byte[]> result = dir.get("k1".getBytes());
            assertTrue(result.isPresent());
            assertArrayEquals(new byte[0], result.get());
        } finally {
            dir.close();
        }
    }

    @Test
    void putRejectsNullValue() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            assertThrows(NullPointerException.class, () -> dir.put("k1".getBytes(), null));
        } finally {
            dir.close();
        }
    }

    @Test
    void deleteMakesKeyInvisible() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.delete("k1".getBytes());
            Optional<byte[]> result = dir.get("k1".getBytes());
            assertFalse(result.isPresent());
        } finally {
            dir.close();
        }
    }

    @Test
    void lookupPreservesTombstoneAsThreeStateResult() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.delete("k1".getBytes());

            var result = dir.lookup("k1".getBytes());

            assertTrue(result.isPresent());
            assertTrue(result.get().isTombstone());
            assertFalse(dir.lookup("missing".getBytes()).isPresent());
            assertFalse(dir.get("k1".getBytes()).isPresent());
        } finally {
            dir.close();
        }
    }

    @Test
    void getMissingKeyReturnsEmpty() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            Optional<byte[]> result = dir.get("nonexistent".getBytes());
            assertFalse(result.isPresent());
        } finally {
            dir.close();
        }
    }

    @Test
    void putOverwritesExisting() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.put("k1".getBytes(), "v2".getBytes());
            assertArrayEquals("v2".getBytes(), dir.get("k1".getBytes()).orElse(null));
        } finally {
            dir.close();
        }
    }

    @Test
    void writeBatchAppliesPutDeleteAndDuplicateKeysInOrder() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.writeBatch(List.of(
                WriteOp.put("k1".getBytes(), "v1".getBytes()),
                WriteOp.put("k2".getBytes(), "v2".getBytes()),
                WriteOp.put("k1".getBytes(), "v1-new".getBytes()),
                WriteOp.delete("k2".getBytes()),
                WriteOp.put("k3".getBytes(), new byte[0])
            ));

            assertArrayEquals("v1-new".getBytes(), dir.get("k1".getBytes()).orElse(null));
            assertFalse(dir.get("k2".getBytes()).isPresent());
            assertTrue(dir.lookup("k2".getBytes()).orElseThrow().isTombstone());
            assertArrayEquals(new byte[0], dir.get("k3".getBytes()).orElseThrow());

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(5L, snap.lastAssignedSequenceId());
            assertEquals(1L, snap.curMemTableMinSequenceId());
            assertEquals(5L, snap.curMemTableMaxSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void writeBatchRejectsInvalidBatchBeforeWriting() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            assertThrows(IllegalArgumentException.class, () -> dir.writeBatch(List.of()));
            assertThrows(NullPointerException.class, () -> dir.writeBatch(null));
            assertThrows(NullPointerException.class, () -> WriteOp.put("k1".getBytes(), null));
            assertThrows(IllegalArgumentException.class, () -> WriteOp.put(new byte[0], "v1".getBytes()));

            assertEquals(0L, dir.stateSnapshot().lastAssignedSequenceId());
            assertFalse(dir.get("k1".getBytes()).isPresent());
        } finally {
            dir.close();
        }
    }

    @Test
    void writeAdmissionRejectsBeforeWalAtImmutableWatermarkAndResumesAfterFlush() throws IOException {
        PMSConfig cfg = withFlowControl(config(1, 256), 1, 20);
        PMSBucketDirectorImpl dir = newDirector(cfg);
        dir.init();
        try {
            dir.put("accepted-before-flush".getBytes(), "v1".getBytes());
            assertEquals(1, dir.stateSnapshot().immutableMemTableCount());

            assertThrows(
                PmsWriteOverloadedException.class,
                () -> dir.put("rejected".getBytes(), "v2".getBytes())
            );
            assertEquals(1, dir.stateSnapshot().lastAssignedSequenceId());
            assertTrue(dir.get("rejected".getBytes()).isEmpty());

            assertTrue(dir.flushImmutableMemTable().progressed());
            dir.put("accepted-after-flush".getBytes(), "v3".getBytes());
            assertEquals(2, dir.stateSnapshot().lastAssignedSequenceId());
        } finally {
            dir.close();
        }

        PMSBucketDirectorImpl recovered = newDirector(cfg);
        recovered.init();
        try {
            assertArrayEquals("v1".getBytes(), recovered.get("accepted-before-flush".getBytes()).orElseThrow());
            assertTrue(recovered.get("rejected".getBytes()).isEmpty());
            assertArrayEquals("v3".getBytes(), recovered.get("accepted-after-flush".getBytes()).orElseThrow());
        } finally {
            recovered.close();
        }
    }

    @Test
    void writeAdmissionRejectsBeforeWalAtNewSstWatermarkAndResumesAfterSink() throws IOException {
        PMSConfig cfg = withFlowControl(config(1, 256), 4, 1);
        PMSBucketDirectorImpl dir = newDirector(cfg);
        dir.init();
        try {
            dir.put("accepted-before-sink".getBytes(), "v1".getBytes());
            assertTrue(dir.flushImmutableMemTable().progressed());
            assertEquals(1, dir.stateSnapshot().newSSTCount());

            assertThrows(
                PmsWriteOverloadedException.class,
                () -> dir.put("rejected".getBytes(), "v2".getBytes())
            );
            assertEquals(1, dir.stateSnapshot().lastAssignedSequenceId());

            assertTrue(dir.sinkToPaimon(allAvailableSinkSelection()).progressed());
            dir.put("accepted-after-sink".getBytes(), "v3".getBytes());
            assertEquals(2, dir.stateSnapshot().lastAssignedSequenceId());
        } finally {
            dir.close();
        }
    }

    // ── Freeze ──

    @Test
    void freezeMovesDataToImmutable() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            FreezeResult freeze = dir.freezeCurMemTable();

            assertTrue(freeze.progressed());
            assertEquals(1L, freeze.fenceSequenceId());
            assertEquals(1L, freeze.frozenMinSequenceId());
            assertEquals(1L, freeze.frozenMaxSequenceId());

            // k1 should still be visible (from immutable layer)
            Optional<byte[]> result = dir.get("k1".getBytes());
            assertTrue(result.isPresent());
            assertArrayEquals("v1".getBytes(), result.get());

            // State snapshot should show 1 immutable
            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(1, snap.immutableMemTableCount());
            assertEquals(0, snap.curMemTableEstimatedEntryCount());
            assertEquals(1L, snap.lastAssignedSequenceId());
            assertEquals(1L, snap.immutableMemTableMinSequenceId());
            assertEquals(1L, snap.immutableMemTableMaxSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void maintenanceOperationsReportNoopWithoutEligibleData() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            FreezeResult freeze = dir.freezeCurMemTable();

            assertFalse(freeze.progressed());
            assertEquals(0L, freeze.fenceSequenceId());
            assertFalse(dir.flushImmutableMemTable().progressed());
            assertFalse(dir.sinkToPaimon(allAvailableSinkSelection()).progressed());
            assertFalse(dir.compactLocalSSTs(new CompactionSelection(SSTState.NEW, List.of(1L, 2L))).progressed());
            assertFalse(dir.evictOldestSinkedSST().progressed());
        } finally {
            dir.close();
        }
    }

    @Test
    void freezeReturnsAtomicFenceForConcurrentWriteBoundary() throws Exception {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        int writerCount = 8;
        CyclicBarrier barrier = new CyclicBarrier(writerCount + 1);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Thread> writers = new ArrayList<>();
        AtomicReference<FreezeResult> freezeRef = new AtomicReference<>();
        try {
            dir.put("before".getBytes(), "v".getBytes());
            for (int i = 0; i < writerCount; i++) {
                int writerId = i;
                writers.add(new Thread(() -> {
                    try {
                        barrier.await();
                        dir.put(("concurrent-" + writerId).getBytes(), "v".getBytes());
                    } catch (Throwable error) {
                        errors.add(error);
                    }
                }));
            }
            writers.forEach(Thread::start);

            barrier.await();
            freezeRef.set(dir.freezeCurMemTable());
            for (Thread writer : writers) {
                writer.join();
            }

            assertTrue(errors.isEmpty(), "Concurrent writes failed: " + errors);
            FreezeResult freeze = freezeRef.get();
            assertTrue(freeze.progressed());
            assertEquals(freeze.fenceSequenceId(), freeze.frozenMaxSequenceId());

            BucketStateSnapshot snapshot = dir.stateSnapshot();
            assertEquals(freeze.fenceSequenceId(), snapshot.immutableMemTableMaxSequenceId());
            assertTrue(
                snapshot.curMemTableMinSequenceId() == 0
                    || snapshot.curMemTableMinSequenceId() > freeze.fenceSequenceId(),
                "The active MemTable must contain only writes after the freeze fence"
            );
            assertEquals(1L + writerCount, snapshot.lastAssignedSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void oldestWriteTimePropagatesAcrossAllLocalLayers() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256, 100, 32, 2));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            long oldestWriteAtMillis = dir.stateSnapshot().curMemTableOldestWriteAtMillis();
            assertTrue(oldestWriteAtMillis > 0);
            dir.put("k2".getBytes(), "v2".getBytes());

            BucketStateSnapshot cur = dir.stateSnapshot();
            assertEquals(oldestWriteAtMillis, cur.curMemTableOldestWriteAtMillis());

            FreezeResult freeze = dir.freezeCurMemTable();
            assertEquals(oldestWriteAtMillis, freeze.oldestWriteAtMillis());
            BucketStateSnapshot immutable = dir.stateSnapshot();
            assertEquals(oldestWriteAtMillis, immutable.immutableMemTableOldestWriteAtMillis());

            LocalRunSnapshot firstRun = dir.flushImmutableMemTable().outputRun().orElseThrow();
            assertEquals(oldestWriteAtMillis, firstRun.oldestWriteAtMillis());

            dir.put("k3".getBytes(), "v3".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            LocalRunSnapshot compacted = compactAllRuns(dir, SSTState.NEW)
                .group().orElseThrow().outputRun();
            assertEquals(oldestWriteAtMillis, compacted.oldestWriteAtMillis());

            LocalRunSnapshot sinked = dir.sinkToPaimon(allAvailableSinkSelection()).sinkedRuns().get(0);
            assertEquals(oldestWriteAtMillis, sinked.oldestWriteAtMillis());
            assertEquals(SSTState.SINKED, sinked.state());

            BucketStateSnapshot finalState = dir.stateSnapshot();
            assertEquals(oldestWriteAtMillis, finalState.sinkedSSTOldestWriteAtMillis());
            assertEquals(3L, finalState.lastPersistedSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void oldestWriteTimeSurvivesLocalSSTRestart() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        long oldestWriteAtMillis = dir1.flushImmutableMemTable()
            .outputRun().orElseThrow().oldestWriteAtMillis();
        dir1.close();

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            BucketStateSnapshot recovered = dir2.stateSnapshot();
            assertEquals(oldestWriteAtMillis, recovered.newSSTOldestWriteAtMillis());
            assertEquals(oldestWriteAtMillis, recovered.localRuns().get(0).oldestWriteAtMillis());
            assertEquals(0L, recovered.lastPersistedSequenceId());
            assertTrue(recovered.recoveredUnpersistedData());

            dir2.sinkToPaimon(allAvailableSinkSelection());
            BucketStateSnapshot persisted = dir2.stateSnapshot();
            assertEquals(1L, persisted.lastPersistedSequenceId());
            assertFalse(persisted.recoveredUnpersistedData());
        } finally {
            dir2.close();
        }
    }

    @Test
    void autoFreezeOnEntryThreshold() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(5, 256));
        dir.init();
        try {
            // Write 6 entries — threshold is 5, should auto-freeze
            for (int i = 0; i < 6; i++) {
                dir.put(("k" + i).getBytes(), ("v" + i).getBytes());
            }

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertTrue(snap.immutableMemTableCount() > 0, "Should have frozen at least one MemTable");
        } finally {
            dir.close();
        }
    }

    @Test
    void autoFreezePreservesSequenceBoundary() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(3, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.delete("k3".getBytes());

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(1, snap.immutableMemTableCount());
            assertEquals(3L, snap.lastAssignedSequenceId());
            assertEquals(1L, snap.immutableMemTableMinSequenceId());
            assertEquals(3L, snap.immutableMemTableMaxSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void writeBatchAutoFreezePreservesWholeBatchBoundary() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(3, 256));
        dir.init();
        try {
            dir.writeBatch(List.of(
                WriteOp.put("k1".getBytes(), "v1".getBytes()),
                WriteOp.put("k2".getBytes(), "v2".getBytes()),
                WriteOp.delete("k3".getBytes()),
                WriteOp.put("k4".getBytes(), "v4".getBytes())
            ));

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(1, snap.immutableMemTableCount());
            assertEquals(0, snap.curMemTableEstimatedEntryCount());
            assertEquals(4L, snap.lastAssignedSequenceId());
            assertEquals(1L, snap.immutableMemTableMinSequenceId());
            assertEquals(4L, snap.immutableMemTableMaxSequenceId());
            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElseThrow());
            assertFalse(dir.get("k3".getBytes()).isPresent());
        } finally {
            dir.close();
        }
    }

    @Test
    void autoFreezeOnSizeThreshold() throws IOException {
        // 1 MB size limit
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 1));
        dir.init();
        try {
            byte[] largeValue = new byte[200 * 1024];
            java.util.Arrays.fill(largeValue, (byte) 'X');
            for (int i = 0; i < 6; i++) {
                dir.put(("k" + i).getBytes(), largeValue);
            }

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertTrue(snap.immutableMemTableCount() > 0, "Should have frozen due to size threshold");
        } finally {
            dir.close();
        }
    }

    @Test
    void getAfterFreezeAndNewWrite() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.put("k2".getBytes(), "v2".getBytes());

            // Both should be visible
            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElse(null));
            assertArrayEquals("v2".getBytes(), dir.get("k2".getBytes()).orElse(null));
        } finally {
            dir.close();
        }
    }

    @Test
    void operationsAfterCloseThrow() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        dir.close();

        assertThrows(IllegalStateException.class, () -> dir.put("k".getBytes(), "v".getBytes()));
        assertThrows(IllegalStateException.class, () -> dir.delete("k".getBytes()));
        assertThrows(IllegalStateException.class, () -> dir.get("k".getBytes()));
        assertThrows(IllegalStateException.class, dir::freezeCurMemTable);
        assertThrows(
            IllegalStateException.class,
            () -> dir.sinkToPaimon(allAvailableSinkSelection())
        );
        assertThrows(
            IllegalStateException.class,
            () -> dir.compactLocalSSTs(new CompactionSelection(SSTState.NEW, List.of(1L, 2L)))
        );
        assertThrows(IllegalStateException.class, dir::stateSnapshot);
        dir.close();
    }

    @Test
    void immutableLayerOverriddenByCurMemTable() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.put("k1".getBytes(), "v2".getBytes());

            // curMemTable should take priority
            assertArrayEquals("v2".getBytes(), dir.get("k1".getBytes()).orElse(null));
        } finally {
            dir.close();
        }
    }

    // ── Flush to SST ──

    @Test
    void flushMovesImmutableToNewSSTAndGetReadsFromSST() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            FlushResult flush = dir.flushImmutableMemTable();

            assertTrue(flush.progressed());
            LocalRunSnapshot output = flush.outputRun().orElseThrow();
            assertEquals(1L, output.runId());
            assertEquals(SSTState.NEW, output.state());

            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElse(null));

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(0, snap.immutableMemTableCount());
            assertEquals(1L, snap.lastFlushedSequenceId());
            assertEquals(1, snap.newSSTCount());
            assertEquals(1L, snap.newSSTMinSequenceId());
            assertEquals(1L, snap.newSSTMaxSequenceId());
            assertTrue(snap.newSSTTotalBytes() > 0);
        } finally {
            dir.close();
        }
    }

    @Test
    void sstTombstoneStopsLookup() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.put("k1".getBytes(), "v2".getBytes());
            dir.delete("k1".getBytes());
            dir.freezeCurMemTable();

            // Flush first immutable with v1, then second immutable with tombstone.
            dir.flushImmutableMemTable();
            dir.flushImmutableMemTable();

            assertFalse(dir.get("k1".getBytes()).isPresent());
            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(2, snap.newSSTCount());
            assertEquals(1L, snap.newSSTMinSequenceId());
            assertEquals(3L, snap.newSSTMaxSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void curMemTableOverridesFlushedSST() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k1".getBytes(), "v2".getBytes());

            assertArrayEquals("v2".getBytes(), dir.get("k1".getBytes()).orElse(null));
        } finally {
            dir.close();
        }
    }

    @Test
    void scanMergesAllLocalLayersByLatestSequenceAndFiltersTombstones() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("p/1".getBytes(), "old-1".getBytes());
            dir.put("p/2".getBytes(), "old-2".getBytes());
            dir.put("q/1".getBytes(), "outside".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.sinkToPaimon(allAvailableSinkSelection());

            dir.put("p/1".getBytes(), "new-1".getBytes());
            dir.delete("p/2".getBytes());
            dir.put("p/3".getBytes(), "new-3".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            dir.put("p/4".getBytes(), "cur-4".getBytes());

            List<Entry> entries = dir.prefixScan("p/".getBytes());

            assertEquals(List.of("p/1", "p/3", "p/4"), keys(entries));
            assertEquals(List.of("new-1", "new-3", "cur-4"), values(entries));
        } finally {
            dir.close();
        }
    }

    @Test
    void scanUsesEndExclusiveAndReturnsKeyOrder() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("a".getBytes(), "va".getBytes());
            dir.put("b".getBytes(), "vb".getBytes());
            dir.put("c".getBytes(), "vc".getBytes());

            List<Entry> entries = dir.scan("b".getBytes(), Optional.of("c".getBytes()));

            assertEquals(List.of("b"), keys(entries));
            assertEquals(List.of("vb"), values(entries));
        } finally {
            dir.close();
        }
    }

    @Test
    void compactLocalSSTsMergesNewRunsWithoutChangingLookupOrder() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256, 100, 32, 2));
        dir.init();
        try {
            dir.put("k1".getBytes(), "old".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k1".getBytes(), "new".getBytes());
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            CompactionResult compaction = compactAllRuns(dir, SSTState.NEW);

            assertTrue(compaction.progressed());
            assertEquals(List.of(1L, 2L), compaction.group().orElseThrow().inputRunIds());
            assertEquals(1L, compaction.group().orElseThrow().outputRun().minFlushId());
            assertEquals(2L, compaction.group().orElseThrow().outputRun().maxFlushId());

            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(1, snap.newSSTCount());
            assertEquals(2L, snap.newSSTTotalRows());
            assertArrayEquals("new".getBytes(), dir.get("k1".getBytes()).orElse(null));
            assertArrayEquals("v2".getBytes(), dir.get("k2".getBytes()).orElse(null));
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000002.sst")));
        } finally {
            dir.close();
        }
    }

    // ── Sink boundary ──

    @Test
    void sinkMovesNewSSTsToSinkedAndKeepsDataReadable() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            Path sstFile = tempDir.resolve("storage").resolve("sst-000001-000001.sst");
            assertTrue(Files.exists(sstFile));

            SinkOperationResult sink = dir.sinkToPaimon(allAvailableSinkSelection());

            assertTrue(sink.progressed());
            assertEquals(1L, sink.commitResult().orElseThrow().snapshotId());
            assertEquals(1, sink.sinkedRuns().size());
            assertEquals(SSTState.SINKED, sink.sinkedRuns().get(0).state());

            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElse(null));
            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(1L, snap.lastSinkedSnapshotId());
            assertTrue(Files.exists(sstFile));
        } finally {
            dir.close();
        }
    }

    @Test
    void sinkUsesInjectedSinkManager() throws IOException {
        RecordingSinkManager sinkManager = new RecordingSinkManager(99);
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256), sinkManager);
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            dir.sinkToPaimon(allAvailableSinkSelection());

            assertEquals(1, sinkManager.prepareCalls);
            assertEquals(1, sinkManager.commitCalls);
            assertEquals("sink-1-1", sinkManager.preparedBatchId);
            assertEquals(1L, sinkManager.preparedMaxSequenceId);
            assertEquals(99L, dir.stateSnapshot().lastSinkedSnapshotId());
        } finally {
            dir.close();
        }
    }

    @Test
    void boundedSinkSelectionAdvancesOneContinuousPrefixTowardFixedFence() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            flushEntry(dir, "k1", "v1");
            flushEntry(dir, "k2", "v2");
            flushEntry(dir, "k3", "v3");

            long firstRunBytes = dir.stateSnapshot().localRuns().get(0).fileSizeBytes();
            SinkOperationResult first = dir.sinkToPaimon(new SinkSelection(3, firstRunBytes));

            assertTrue(first.progressed());
            assertEquals(List.of(1L), first.sinkedRuns().stream().map(LocalRunSnapshot::runId).toList());
            assertEquals(1L, dir.stateSnapshot().lastPersistedSequenceId());

            SinkOperationResult second = dir.sinkToPaimon(new SinkSelection(2, Long.MAX_VALUE));

            assertTrue(second.progressed());
            assertEquals(List.of(2L), second.sinkedRuns().stream().map(LocalRunSnapshot::runId).toList());
            BucketStateSnapshot atFence = dir.stateSnapshot();
            assertEquals(2L, atFence.lastPersistedSequenceId());
            assertEquals(1, atFence.newSSTCount());
            assertEquals(3L, atFence.newSSTMinSequenceId());
            assertFalse(dir.sinkToPaimon(new SinkSelection(2, Long.MAX_VALUE)).progressed());

            assertTrue(dir.sinkToPaimon(allAvailableSinkSelection()).progressed());
            assertEquals(3L, dir.stateSnapshot().lastPersistedSequenceId());
        } finally {
            dir.close();
        }
    }

    @Test
    void sinkByteLimitStillAllowsOneOversizedOldestRun() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            flushEntry(dir, "k1", "v1");
            flushEntry(dir, "k2", "v2");
            flushEntry(dir, "k3", "v3");
            List<LocalRunSnapshot> runs = dir.stateSnapshot().localRuns();
            long belowFirstTwo = runs.get(0).fileSizeBytes() + runs.get(1).fileSizeBytes() - 1;

            SinkOperationResult first = dir.sinkToPaimon(
                new SinkSelection(Long.MAX_VALUE, belowFirstTwo)
            );

            assertEquals(List.of(1L), first.sinkedRuns().stream().map(LocalRunSnapshot::runId).toList());

            SinkOperationResult oversized = dir.sinkToPaimon(
                new SinkSelection(Long.MAX_VALUE, 1)
            );

            assertEquals(List.of(2L), oversized.sinkedRuns().stream().map(LocalRunSnapshot::runId).toList());
            assertEquals(1, dir.stateSnapshot().newSSTCount());
        } finally {
            dir.close();
        }
    }

    @Test
    void sinkSelectionRejectsRunCrossingTargetSequenceFence() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> dir.sinkToPaimon(new SinkSelection(1, Long.MAX_VALUE))
            );

            assertTrue(error.getMessage().contains("crosses Sink target sequence fence"));
            assertEquals(1, dir.stateSnapshot().newSSTCount());
            assertEquals(SinkFlightSnapshot.Status.IDLE, dir.stateSnapshot().sinkFlight().status());
        } finally {
            dir.close();
        }
    }

    @Test
    void flushPublishedDuringSinkRemainsNewForNextSink() throws Exception {
        BlockingPrepareSinkManager sinkManager = new BlockingPrepareSinkManager();
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256), sinkManager);
        dir.init();
        AtomicReference<Throwable> sinkFailure = new AtomicReference<>();
        Thread sinkThread = new Thread(() -> {
            try {
                dir.sinkToPaimon(allAvailableSinkSelection());
            } catch (Throwable failure) {
                sinkFailure.set(failure);
            }
        });
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            sinkThread.start();
            assertTrue(sinkManager.prepareEntered.await(5, TimeUnit.SECONDS));

            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            sinkManager.allowPrepare.countDown();
            sinkThread.join(TimeUnit.SECONDS.toMillis(5));

            assertFalse(sinkThread.isAlive(), "sink thread did not finish");
            assertNull(sinkFailure.get());
            BucketStateSnapshot afterFirstSink = dir.stateSnapshot();
            assertEquals(1, afterFirstSink.newSSTCount());
            assertEquals(1, afterFirstSink.sinkedSSTCount());
            assertEquals(2L, afterFirstSink.newSSTMinSequenceId());
            assertEquals(2L, afterFirstSink.newSSTMaxSequenceId());

            assertTrue(dir.sinkToPaimon(allAvailableSinkSelection()).progressed());
            BucketStateSnapshot afterSecondSink = dir.stateSnapshot();
            assertEquals(0, afterSecondSink.newSSTCount());
            assertEquals(2, afterSecondSink.sinkedSSTCount());
            assertEquals(2L, afterSecondSink.lastPersistedSequenceId());
        } finally {
            sinkManager.allowPrepare.countDown();
            sinkThread.join(TimeUnit.SECONDS.toMillis(5));
            dir.close();
        }
    }

    @Test
    void compactedSinkedRunRecoversAsSinkedAfterRestart() throws IOException {
        PMSConfig cfg = config(1_000_000, 256, 100, 32, 2);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        try {
            dir1.put("k1".getBytes(), "v1".getBytes());
            dir1.freezeCurMemTable();
            dir1.flushImmutableMemTable();
            dir1.put("k2".getBytes(), "v2".getBytes());
            dir1.freezeCurMemTable();
            dir1.flushImmutableMemTable();
            dir1.sinkToPaimon(allAvailableSinkSelection());

            compactAllRuns(dir1, SSTState.SINKED);

            BucketStateSnapshot compacted = dir1.stateSnapshot();
            assertEquals(0, compacted.newSSTCount());
            assertEquals(1, compacted.sinkedSSTCount());
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000002.sst")));
        } finally {
            dir1.close();
        }

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            BucketStateSnapshot recovered = dir2.stateSnapshot();
            assertEquals(0, recovered.newSSTCount());
            assertEquals(1, recovered.sinkedSSTCount());
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));
            assertArrayEquals("v2".getBytes(), dir2.get("k2".getBytes()).orElse(null));
        } finally {
            dir2.close();
        }
    }

    @Test
    void explicitCompactionRejectsNonContinuousInputsAndNoopsWhenStale() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256, 100, 32, 2));
        dir.init();
        try {
            flushEntry(dir, "k1", "v1");
            flushEntry(dir, "k2", "v2");
            flushEntry(dir, "k3", "v3");

            assertThrows(
                IllegalStateException.class,
                () -> compactRuns(dir, SSTState.NEW, List.of(1L, 3L))
            );
            assertFalse(compactRuns(dir, SSTState.NEW, List.of(98L, 99L)).progressed());

            assertTrue(compactRuns(dir, SSTState.NEW, List.of(1L, 2L)).progressed());
            assertFalse(compactRuns(dir, SSTState.NEW, List.of(1L, 2L)).progressed());
            assertEquals(2, dir.stateSnapshot().newSSTCount());
        } finally {
            dir.close();
        }
    }

    @Test
    void explicitSinkedCompactionCanResolveCountPressureWithoutEviction() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256, 1, 32, 2));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.sinkToPaimon(allAvailableSinkSelection());

            CompactionResult compaction = compactAllRuns(dir, SSTState.SINKED);

            assertTrue(compaction.progressed());
            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(1, snap.sinkedSSTCount());
            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElse(null));
            assertArrayEquals("v2".getBytes(), dir.get("k2".getBytes()).orElse(null));
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000002.sst")));
        } finally {
            dir.close();
        }
    }

    @Test
    void evictOldestSinkedSSTDeletesOnlyOldestSinkedFile() throws IOException {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.sinkToPaimon(allAvailableSinkSelection());

            dir.put("k3".getBytes(), "v3".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            BucketStateSnapshot before = dir.stateSnapshot();
            assertEquals(1, before.newSSTCount());
            assertEquals(1L, before.newSSTTotalRows());
            assertEquals(2, before.sinkedSSTCount());
            assertEquals(2L, before.sinkedSSTTotalRows());

            Path oldestSinked = tempDir.resolve("storage").resolve("sst-000001-000001.sst");
            Path newerSinked = tempDir.resolve("storage").resolve("sst-000002-000002.sst");
            Path unsinked = tempDir.resolve("storage").resolve("sst-000003-000003.sst");
            assertTrue(Files.exists(oldestSinked));
            assertTrue(Files.exists(newerSinked));
            assertTrue(Files.exists(unsinked));

            EvictionResult eviction = dir.evictOldestSinkedSST();

            assertTrue(eviction.progressed());
            assertEquals(1L, eviction.evictedRun().orElseThrow().runId());
            assertFalse(Files.exists(oldestSinked));
            assertTrue(Files.exists(newerSinked));
            assertTrue(Files.exists(unsinked));
            assertFalse(dir.get("k1".getBytes()).isPresent());
            assertArrayEquals("v2".getBytes(), dir.get("k2".getBytes()).orElse(null));
            assertArrayEquals("v3".getBytes(), dir.get("k3".getBytes()).orElse(null));

            BucketStateSnapshot after = dir.stateSnapshot();
            assertEquals(1, after.newSSTCount());
            assertEquals(1L, after.newSSTTotalRows());
            assertEquals(1, after.sinkedSSTCount());
            assertEquals(1L, after.sinkedSSTTotalRows());
        } finally {
            dir.close();
        }
    }

    @Test
    void restartRecoversSinkedSSTFromSuccessMetadata() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.flushImmutableMemTable();
        dir1.sinkToPaimon(allAvailableSinkSelection());
        dir1.close();

        Path sstFile = tempDir.resolve("storage").resolve("sst-000001-000001.sst");
        assertTrue(Files.exists(sstFile));

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));
            BucketStateSnapshot snap = dir2.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(1L, snap.lastSinkedSnapshotId());
            assertTrue(Files.exists(sstFile));
        } finally {
            dir2.close();
        }
    }

    @Test
    void restartRetriesPreparedSinkWithoutSuccess() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        FailingCommitSinkManager failingSink = new FailingCommitSinkManager();
        PMSBucketDirectorImpl dir1 = newDirector(cfg, failingSink);
        dir1.init();
        try {
            dir1.put("k1".getBytes(), "v1".getBytes());
            dir1.freezeCurMemTable();
            dir1.flushImmutableMemTable();

            RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> dir1.sinkToPaimon(allAvailableSinkSelection())
            );
            assertTrue(error.getMessage().contains("commit failed after prepare"));
            assertEquals(1, failingSink.prepareCalls);
            assertEquals(1, failingSink.commitCalls);

            BucketStateSnapshot failedSink = dir1.stateSnapshot();
            assertEquals(SinkFlightSnapshot.Status.PREPARED_RETRY, failedSink.sinkFlight().status());
            assertEquals("sink-1-1", failedSink.sinkFlight().batchId());
            assertEquals(1L, failedSink.sinkFlight().sinkFenceFlushId());

            RuntimeException retryError = assertThrows(RuntimeException.class, dir1::commitPreparedSink);
            assertTrue(retryError.getMessage().contains("commit failed after prepare"));
            assertEquals(1, failingSink.prepareCalls);
            assertEquals(2, failingSink.commitCalls);
            assertEquals(
                SinkFlightSnapshot.Status.PREPARED_RETRY,
                dir1.stateSnapshot().sinkFlight().status()
            );
        } finally {
            dir1.close();
        }

        Path sstFile = tempDir.resolve("storage").resolve("sst-000001-000001.sst");
        assertTrue(Files.exists(sstFile));

        RecordingSinkManager recoveringSink = new RecordingSinkManager(77);
        PMSBucketDirectorImpl dir2 = newDirector(cfg, recoveringSink);
        dir2.init();
        try {
            assertEquals(0, recoveringSink.prepareCalls);
            assertEquals(1, recoveringSink.commitCalls);
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));

            BucketStateSnapshot snap = dir2.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(77L, snap.lastSinkedSnapshotId());
            assertEquals(SinkFlightSnapshot.Status.IDLE, snap.sinkFlight().status());
            assertTrue(Files.exists(sstFile));
        } finally {
            dir2.close();
        }

        RecordingSinkManager alreadyRecoveredSink = new RecordingSinkManager(88);
        PMSBucketDirectorImpl dir3 = newDirector(cfg, alreadyRecoveredSink);
        dir3.init();
        try {
            assertEquals(0, alreadyRecoveredSink.prepareCalls);
            assertEquals(0, alreadyRecoveredSink.commitCalls);
            assertEquals(77L, dir3.stateSnapshot().lastSinkedSnapshotId());
        } finally {
            dir3.close();
        }
    }

    @Test
    void preparedSinkCommitCanRetryInSameProcessWithoutPreparingAgain() throws IOException {
        FailOnceCommitSinkManager sinkManager = new FailOnceCommitSinkManager(77);
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256), sinkManager);
        dir.init();
        try {
            flushEntry(dir, "k1", "v1");

            RuntimeException firstFailure = assertThrows(
                RuntimeException.class,
                () -> dir.sinkToPaimon(allAvailableSinkSelection())
            );
            assertTrue(firstFailure.getMessage().contains("commit failed once after prepare"));
            assertEquals(1, sinkManager.prepareCalls);
            assertEquals(1, sinkManager.commitCalls);
            assertEquals(
                SinkFlightSnapshot.Status.PREPARED_RETRY,
                dir.stateSnapshot().sinkFlight().status()
            );

            flushEntry(dir, "k2", "v2");

            SinkOperationResult retried = dir.commitPreparedSink();

            assertTrue(retried.progressed());
            assertEquals(77L, retried.commitResult().orElseThrow().snapshotId());
            assertEquals(1, sinkManager.prepareCalls);
            assertEquals(2, sinkManager.commitCalls);
            assertEquals(sinkManager.firstCommit.batchId(), sinkManager.secondCommit.batchId());
            assertEquals(sinkManager.firstCommit.commitIdentifier(), sinkManager.secondCommit.commitIdentifier());
            assertEquals(sinkManager.firstCommit.sstIds(), sinkManager.secondCommit.sstIds());
            assertArrayEquals(sinkManager.firstCommit.payload(), sinkManager.secondCommit.payload());
            assertEquals(sinkManager.firstCommit.fileRefs(), sinkManager.secondCommit.fileRefs());

            BucketStateSnapshot recovered = dir.stateSnapshot();
            assertEquals(SinkFlightSnapshot.Status.IDLE, recovered.sinkFlight().status());
            assertEquals(1, recovered.newSSTCount());
            assertEquals(1, recovered.sinkedSSTCount());
            assertEquals(1L, recovered.lastPersistedSequenceId());
            assertEquals(77L, recovered.lastSinkedSnapshotId());
            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElseThrow());
            assertArrayEquals("v2".getBytes(), dir.get("k2".getBytes()).orElseThrow());
            assertFalse(dir.commitPreparedSink().progressed());
        } finally {
            dir.close();
        }
    }

    @Test
    void preparedSinkFenceProtectsSelectedPrefixButAllowsNewerCompaction() throws IOException {
        PMSConfig cfg = config(1_000_000, 256, 100, 32, 2);
        FailingCommitSinkManager failingSink = new FailingCommitSinkManager();
        PMSBucketDirectorImpl dir = newDirector(cfg, failingSink);
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            assertThrows(
                RuntimeException.class,
                () -> dir.sinkToPaimon(allAvailableSinkSelection())
            );
            assertEquals(1L, dir.stateSnapshot().sinkFlight().sinkFenceFlushId());
            assertThrows(
                IllegalStateException.class,
                () -> dir.sinkToPaimon(allAvailableSinkSelection())
            );

            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k3".getBytes(), "v3".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            CompactionResult compaction = compactRuns(dir, SSTState.NEW, List.of(2L, 3L));

            assertTrue(compaction.progressed());
            assertEquals(List.of(2L, 3L), compaction.group().orElseThrow().inputRunIds());
            assertEquals(2L, compaction.group().orElseThrow().outputRun().minFlushId());
            assertEquals(3L, compaction.group().orElseThrow().outputRun().maxFlushId());
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000001.sst")));
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000002-000003.sst")));
            assertEquals(SinkFlightSnapshot.Status.PREPARED_RETRY, dir.stateSnapshot().sinkFlight().status());
        } finally {
            dir.close();
        }
    }

    // ── Recovery ──

    @Test
    void recoverFromWALAfterRestart() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        // Write data, close
        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.put("k2".getBytes(), "v2".getBytes());
        dir1.delete("k1".getBytes());
        dir1.close();

        // Re-open and verify recovery
        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            assertFalse(dir2.get("k1".getBytes()).isPresent(), "k1 was deleted");
            assertTrue(dir2.get("k2".getBytes()).isPresent(), "k2 should be recovered");
            assertArrayEquals("v2".getBytes(), dir2.get("k2".getBytes()).orElse(null));
            BucketStateSnapshot snapshot = dir2.stateSnapshot();
            assertEquals(3L, snapshot.lastAssignedSequenceId());
            assertEquals(0L, snapshot.lastPersistedSequenceId());
            assertTrue(snapshot.recoveredUnpersistedData());
        } finally {
            dir2.close();
        }
    }

    @Test
    void recoverWriteBatchFromWALAfterRestart() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.writeBatch(List.of(
            WriteOp.put("k1".getBytes(), "v1".getBytes()),
            WriteOp.put("k2".getBytes(), "v2".getBytes()),
            WriteOp.delete("k1".getBytes()),
            WriteOp.put("k3".getBytes(), "v3".getBytes())
        ));
        dir1.close();

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            assertFalse(dir2.get("k1".getBytes()).isPresent());
            assertTrue(dir2.lookup("k1".getBytes()).orElseThrow().isTombstone());
            assertArrayEquals("v2".getBytes(), dir2.get("k2".getBytes()).orElseThrow());
            assertArrayEquals("v3".getBytes(), dir2.get("k3".getBytes()).orElseThrow());
            assertEquals(4L, dir2.stateSnapshot().lastAssignedSequenceId());
        } finally {
            dir2.close();
        }
    }

    @Test
    void recoverWithFreezeAndOverwrite() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.put("k1".getBytes(), "v2".getBytes());
        dir1.close();

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            // After recovery, k1=v2 (WAL replays all, latest wins)
            assertArrayEquals("v2".getBytes(), dir2.get("k1".getBytes()).orElse(null));
        } finally {
            dir2.close();
        }
    }

    @Test
    void recoverSkipsWalRecordsCoveredByFlushedSST() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.flushImmutableMemTable();
        dir1.put("k2".getBytes(), "v2".getBytes());
        dir1.close();

        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));
            assertArrayEquals("v2".getBytes(), dir2.get("k2".getBytes()).orElse(null));

            BucketStateSnapshot snap = dir2.stateSnapshot();
            assertEquals(1L, snap.lastFlushedSequenceId());
            assertEquals(1, snap.newSSTCount());
            assertEquals(1, snap.curMemTableEstimatedEntryCount());
        } finally {
            dir2.close();
        }
    }

    // ── End-to-end ──

    @Test
    void concurrentPutTriggersAutoFreeze() throws Exception {
        // Low threshold to trigger frequent freezes
        PMSBucketDirectorImpl dir = newDirector(config(10, 256));
        dir.init();

        int threadCount = 4;
        int opsPerThread = 500;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            threads.add(new Thread(() -> {
                try { barrier.await(); } catch (Exception e) { return; }
                for (int i = 0; i < opsPerThread; i++) {
                    try {
                        dir.put(("t" + tid + "-" + i).getBytes(), ("v" + tid + "-" + i).getBytes());
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            }));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        try {
            assertTrue(errors.isEmpty(), "Errors during concurrent put+freeze: " + errors);

            BucketStateSnapshot snap = dir.stateSnapshot();
            // With 2000 total writes and threshold 10, there should be multiple freezes
            assertTrue(snap.immutableMemTableCount() > 0,
                "Should have frozen at least one MemTable with threshold=10 and 2000 writes");
            assertEquals((long) threadCount * opsPerThread, snap.lastAssignedSequenceId());

            // Verify data integrity: all written keys should be readable
            for (int t = 0; t < threadCount; t++) {
                final int tid = t;
                for (int i = 0; i < opsPerThread; i++) {
                    Optional<byte[]> result = dir.get(("t" + tid + "-" + i).getBytes());
                    assertTrue(result.isPresent(),
                        "Key t" + tid + "-" + i + " should be found after concurrent writes");
                    assertArrayEquals(("v" + tid + "-" + i).getBytes(), result.get(),
                        "Key t" + tid + "-" + i + " returned wrong value");
                }
            }
        } finally {
            dir.close();
        }
    }

    @Test
    void lookupRemainsVisibleAcrossConcurrentFreezePublication() throws Exception {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        int rounds = 1_000;
        CyclicBarrier startRound = new CyclicBarrier(2);
        CyclicBarrier endRound = new CyclicBarrier(2);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        Thread freezer = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    dir.put(("freeze-key-" + i).getBytes(), ("value-" + i).getBytes());
                    startRound.await();
                    dir.freezeCurMemTable();
                    endRound.await();
                }
            } catch (Throwable failure) {
                errors.add(failure);
            }
        });
        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    startRound.await();
                    Optional<byte[]> value = dir.get(("freeze-key-" + i).getBytes());
                    if (value.isEmpty()) {
                        errors.add(new AssertionError("lookup missed key during freeze round " + i));
                    }
                    endRound.await();
                }
            } catch (Throwable failure) {
                errors.add(failure);
            }
        });

        freezer.start();
        reader.start();
        freezer.join(TimeUnit.SECONDS.toMillis(30));
        reader.join(TimeUnit.SECONDS.toMillis(30));
        boolean freezerTimedOut = freezer.isAlive();
        boolean readerTimedOut = reader.isAlive();
        if (freezerTimedOut || readerTimedOut) {
            startRound.reset();
            endRound.reset();
            freezer.interrupt();
            reader.interrupt();
            freezer.join(TimeUnit.SECONDS.toMillis(5));
            reader.join(TimeUnit.SECONDS.toMillis(5));
        }
        try {
            assertFalse(freezerTimedOut, "freeze test thread did not finish");
            assertFalse(readerTimedOut, "lookup test thread did not finish");
            assertTrue(errors.isEmpty(), "Errors during concurrent lookup+freeze: " + errors);
        } finally {
            dir.close();
        }
    }

    @Test
    void sstMaintenanceNeverAcquiresWriteBoundaryMutex() throws Exception {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256, 100, 32, 2));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();
            dir.put("k2".getBytes(), "v2".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            assertCompletesWhileHoldingWriteMutex(dir, () ->
                assertTrue(compactAllRuns(dir, SSTState.NEW).progressed())
            );
            assertCompletesWhileHoldingWriteMutex(dir, () ->
                assertTrue(dir.sinkToPaimon(allAvailableSinkSelection()).progressed())
            );
            assertCompletesWhileHoldingWriteMutex(dir, () ->
                assertTrue(dir.evictOldestSinkedSST().progressed())
            );

            BucketStateSnapshot state = dir.stateSnapshot();
            assertEquals(0, state.newSSTCount());
            assertEquals(0, state.sinkedSSTCount());
        } finally {
            dir.close();
        }
    }

    @Test
    void tombstoneRemainsVisibleAcrossConcurrentFreezePublication() throws Exception {
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256));
        dir.init();
        int rounds = 500;
        CyclicBarrier startRound = new CyclicBarrier(2);
        CyclicBarrier endRound = new CyclicBarrier(2);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        Thread freezer = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    dir.delete(("deleted-key-" + i).getBytes());
                    startRound.await();
                    dir.freezeCurMemTable();
                    endRound.await();
                }
            } catch (Throwable failure) {
                errors.add(failure);
            }
        });
        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    startRound.await();
                    Optional<Value> value = dir.lookup(("deleted-key-" + i).getBytes());
                    if (value.isEmpty() || !value.get().isTombstone()) {
                        errors.add(new AssertionError("lookup missed tombstone during freeze round " + i));
                    }
                    endRound.await();
                }
            } catch (Throwable failure) {
                errors.add(failure);
            }
        });

        freezer.start();
        reader.start();
        freezer.join(TimeUnit.SECONDS.toMillis(30));
        reader.join(TimeUnit.SECONDS.toMillis(30));
        boolean freezerTimedOut = freezer.isAlive();
        boolean readerTimedOut = reader.isAlive();
        if (freezerTimedOut || readerTimedOut) {
            startRound.reset();
            endRound.reset();
            freezer.interrupt();
            reader.interrupt();
            freezer.join(TimeUnit.SECONDS.toMillis(5));
            reader.join(TimeUnit.SECONDS.toMillis(5));
        }
        try {
            assertFalse(freezerTimedOut, "freeze test thread did not finish");
            assertFalse(readerTimedOut, "lookup test thread did not finish");
            assertTrue(errors.isEmpty(), "Errors during concurrent tombstone lookup+freeze: " + errors);
        } finally {
            dir.close();
        }
    }

    @Test
    void closeWaitsForInFlightSinkLifecycleLease() throws Exception {
        BlockingPrepareSinkManager sinkManager = new BlockingPrepareSinkManager();
        PMSBucketDirectorImpl dir = newDirector(config(1_000_000, 256), sinkManager);
        dir.init();
        dir.put("k1".getBytes(), "v1".getBytes());
        dir.freezeCurMemTable();
        dir.flushImmutableMemTable();

        AtomicReference<Throwable> sinkFailure = new AtomicReference<>();
        Thread sinkThread = new Thread(() -> {
            try {
                dir.sinkToPaimon(allAvailableSinkSelection());
            } catch (Throwable failure) {
                sinkFailure.set(failure);
            }
        });
        CountDownLatch closeFinished = new CountDownLatch(1);
        Thread closeThread = new Thread(() -> {
            dir.close();
            closeFinished.countDown();
        });

        try {
            sinkThread.start();
            assertTrue(sinkManager.prepareEntered.await(5, TimeUnit.SECONDS));
            closeThread.start();
            assertFalse(
                closeFinished.await(100, TimeUnit.MILLISECONDS),
                "close must wait while Sink still holds a lifecycle lease"
            );

            sinkManager.allowPrepare.countDown();
            sinkThread.join(TimeUnit.SECONDS.toMillis(5));
            closeThread.join(TimeUnit.SECONDS.toMillis(5));

            assertFalse(sinkThread.isAlive(), "sink thread did not finish");
            assertFalse(closeThread.isAlive(), "close thread did not finish");
            assertNull(sinkFailure.get());
            assertThrows(IllegalStateException.class, dir::stateSnapshot);
        } finally {
            sinkManager.allowPrepare.countDown();
            dir.close();
        }
    }

    @Test
    void fullLifecycleWriteFreezeOverwriteRecover() throws IOException {
        PMSConfig cfg = config(3, 256);

        PMSBucketDirectorImpl dir1 = newDirector(cfg);
        dir1.init();
        // Write enough to trigger auto-freeze
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.put("k2".getBytes(), "v2".getBytes());
        dir1.put("k3".getBytes(), "v3".getBytes());
        // 4th write triggers freeze (threshold=3), k4 goes to new curMemTable
        dir1.put("k4".getBytes(), "v4".getBytes());
        // Overwrite k1 in the new curMemTable
        dir1.put("k1".getBytes(), "v1_new".getBytes());
        // Delete k2
        dir1.delete("k2".getBytes());
        dir1.close();

        // Recover
        PMSBucketDirectorImpl dir2 = newDirector(cfg);
        dir2.init();
        try {
            assertArrayEquals("v1_new".getBytes(), dir2.get("k1".getBytes()).orElse(null));
            assertFalse(dir2.get("k2".getBytes()).isPresent());
            assertArrayEquals("v3".getBytes(), dir2.get("k3".getBytes()).orElse(null));
            assertArrayEquals("v4".getBytes(), dir2.get("k4".getBytes()).orElse(null));
        } finally {
            dir2.close();
        }
    }

    private static List<String> keys(List<Entry> entries) {
        return entries.stream()
            .map(entry -> new String(entry.key().bytes(), StandardCharsets.UTF_8))
            .toList();
    }

    private static CompactionResult compactAllRuns(PMSBucketDirectorImpl director, SSTState state) {
        List<Long> runIds = director.stateSnapshot().localRuns().stream()
            .filter(run -> run.state() == state)
            .map(LocalRunSnapshot::runId)
            .toList();
        return compactRuns(director, state, runIds);
    }

    private static void flushEntry(
            PMSBucketDirectorImpl director,
            String key,
            String value) {
        director.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        director.freezeCurMemTable();
        director.flushImmutableMemTable();
    }

    private static CompactionResult compactRuns(
            PMSBucketDirectorImpl director,
            SSTState state,
            List<Long> runIds) {
        return director.compactLocalSSTs(new CompactionSelection(state, runIds));
    }

    private static SinkSelection allAvailableSinkSelection() {
        return new SinkSelection(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    private static List<String> values(List<Entry> entries) {
        return entries.stream()
            .map(entry -> new String(entry.value().bytes(), StandardCharsets.UTF_8))
            .toList();
    }

    private static void assertCompletesWhileHoldingWriteMutex(
            PMSBucketDirectorImpl director,
            Runnable operation) throws Exception {
        Field field = PMSBucketDirectorImpl.class.getDeclaredField("writeMutex");
        field.setAccessible(true);
        Object writeMutex = field.get(director);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            started.countDown();
            try {
                operation.run();
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                finished.countDown();
            }
        });

        boolean completedWhileHeld;
        synchronized (writeMutex) {
            worker.start();
            assertTrue(started.await(5, TimeUnit.SECONDS), "maintenance worker did not start");
            completedWhileHeld = finished.await(5, TimeUnit.SECONDS);
        }
        worker.join(TimeUnit.SECONDS.toMillis(5));

        assertTrue(completedWhileHeld, "SST maintenance attempted to acquire writeMutex");
        assertFalse(worker.isAlive(), "SST maintenance worker did not finish");
        assertNull(failure.get(), "SST maintenance failed: " + failure.get());
    }

    private static class RecordingSinkManager implements SinkManager {
        final long snapshotId;
        int prepareCalls;
        int commitCalls;
        String preparedBatchId;
        long preparedMaxSequenceId;

        RecordingSinkManager(long snapshotId) {
            this.snapshotId = snapshotId;
        }

        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) {
            prepareCalls++;
            preparedBatchId = batch.batchId();
            preparedMaxSequenceId = batch.maxSequenceId();
            List<Long> sstIds = batch.ssts().stream().map(SSTMeta::runId).toList();
            long inputRecordCount = batch.ssts().stream().mapToLong(SSTMeta::entryCount).sum();
            return new PreparedSinkCommit(
                batch.batchId(),
                batch.maxSequenceId(),
                sstIds,
                batch.minSequenceId(),
                batch.maxSequenceId(),
                ("recording-prepare:" + batch.batchId()).getBytes(StandardCharsets.UTF_8),
                List.of(),
                inputRecordCount,
                inputRecordCount
            );
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            commitCalls++;
            return new SinkCommitResult(
                prepared.batchId(),
                snapshotId,
                prepared.maxSequenceId(),
                prepared.sstIds()
            );
        }
    }

    private static final class FailingCommitSinkManager extends RecordingSinkManager {
        FailingCommitSinkManager() {
            super(1);
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            commitCalls++;
            throw new RuntimeException("commit failed after prepare");
        }
    }

    private static final class FailOnceCommitSinkManager extends RecordingSinkManager {
        PreparedSinkCommit firstCommit;
        PreparedSinkCommit secondCommit;

        FailOnceCommitSinkManager(long snapshotId) {
            super(snapshotId);
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            commitCalls++;
            if (firstCommit == null) {
                firstCommit = prepared;
                throw new RuntimeException("commit failed once after prepare");
            }
            secondCommit = prepared;
            return new SinkCommitResult(
                prepared.batchId(),
                snapshotId,
                prepared.maxSequenceId(),
                prepared.sstIds()
            );
        }
    }

    private static final class BlockingPrepareSinkManager extends RecordingSinkManager {
        private final CountDownLatch prepareEntered = new CountDownLatch(1);
        private final CountDownLatch allowPrepare = new CountDownLatch(1);

        BlockingPrepareSinkManager() {
            super(1);
        }

        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) {
            prepareEntered.countDown();
            try {
                if (!allowPrepare.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to release Sink prepare");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Sink prepare interrupted", e);
            }
            return super.prepare(batch);
        }
    }
}
