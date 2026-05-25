package org.qwh.pms.core.bucket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.config.*;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.SSTMeta;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.*;

class PMSBucketDirectorImplTest {

    @TempDir
    Path tempDir;

    private PMSConfig config(int memtableMaxEntries, int memtableMaxSizeMb) {
        return new PMSConfig(
            new MemTableConfig(memtableMaxEntries, memtableMaxSizeMb),
            new WalConfig(tempDir.resolve("wal").toString(), 256, false),
            new StorageConfig(tempDir.resolve("storage").toString(), 0, 0, 0, 0),
            new SinkConfig(0, 0),
            new FlowControlConfig(0, 0),
            new PaimonConfig("dummy", null)
        );
    }

    // ── Write path ──

    @Test
    void putAndGet() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        try {
            assertThrows(NullPointerException.class, () -> dir.put("k1".getBytes(), null));
        } finally {
            dir.close();
        }
    }

    @Test
    void deleteMakesKeyInvisible() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
    void getMissingKeyReturnsEmpty() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.put("k1".getBytes(), "v2".getBytes());
            assertArrayEquals("v2".getBytes(), dir.get("k1".getBytes()).orElse(null));
        } finally {
            dir.close();
        }
    }

    // ── Freeze ──

    @Test
    void freezeMovesDataToImmutable() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();

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
    void autoFreezeOnEntryThreshold() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(5, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(3, 256));
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
    void autoFreezeOnSizeThreshold() throws IOException {
        // 1 MB size limit
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 1));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        dir.close();

        assertThrows(IllegalStateException.class, () -> dir.put("k".getBytes(), "v".getBytes()));
        assertThrows(IllegalStateException.class, () -> dir.delete("k".getBytes()));
        assertThrows(IllegalStateException.class, () -> dir.get("k".getBytes()));
        assertThrows(IllegalStateException.class, dir::freezeCurMemTable);
        assertThrows(IllegalStateException.class, dir::sinkToPaimon);
        assertThrows(IllegalStateException.class, dir::stateSnapshot);
        dir.close();
    }

    @Test
    void immutableLayerOverriddenByCurMemTable() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
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

    // ── Sink boundary ──

    @Test
    void sinkMovesNewSSTsToSinkedAndKeepsDataReadable() throws IOException {
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256));
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            Path newFile = tempDir.resolve("storage").resolve("sst-000001.new.sst");
            Path sinkedFile = tempDir.resolve("storage").resolve("sst-000001.sinked.sst");
            assertTrue(Files.exists(newFile));

            dir.sinkToPaimon();

            assertArrayEquals("v1".getBytes(), dir.get("k1".getBytes()).orElse(null));
            BucketStateSnapshot snap = dir.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(1L, snap.lastSinkedSnapshotId());
            assertTrue(Files.exists(sinkedFile));
        } finally {
            dir.close();
        }
    }

    @Test
    void sinkUsesInjectedSinkManager() throws IOException {
        RecordingSinkManager sinkManager = new RecordingSinkManager(99);
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(1_000_000, 256), sinkManager);
        dir.init();
        try {
            dir.put("k1".getBytes(), "v1".getBytes());
            dir.freezeCurMemTable();
            dir.flushImmutableMemTable();

            dir.sinkToPaimon();

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
    void restartRecoversSinkedSSTFromWalAndRepairsFileNameLabel() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.flushImmutableMemTable();
        dir1.sinkToPaimon();
        dir1.close();

        Path newFile = tempDir.resolve("storage").resolve("sst-000001.new.sst");
        Path sinkedFile = tempDir.resolve("storage").resolve("sst-000001.sinked.sst");
        Files.move(sinkedFile, newFile, StandardCopyOption.REPLACE_EXISTING);

        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg);
        dir2.init();
        try {
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));
            BucketStateSnapshot snap = dir2.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(1L, snap.lastSinkedSnapshotId());
            assertFalse(Files.exists(newFile));
            assertTrue(Files.exists(sinkedFile));
        } finally {
            dir2.close();
        }
    }

    @Test
    void restartRetriesPreparedSinkWithoutSuccess() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        FailingCommitSinkManager failingSink = new FailingCommitSinkManager();
        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg, failingSink);
        dir1.init();
        try {
            dir1.put("k1".getBytes(), "v1".getBytes());
            dir1.freezeCurMemTable();
            dir1.flushImmutableMemTable();

            RuntimeException error = assertThrows(RuntimeException.class, dir1::sinkToPaimon);
            assertTrue(error.getMessage().contains("commit failed after prepare"));
            assertEquals(1, failingSink.prepareCalls);
            assertEquals(1, failingSink.commitCalls);
        } finally {
            dir1.close();
        }

        Path newFile = tempDir.resolve("storage").resolve("sst-000001.new.sst");
        Path sinkedFile = tempDir.resolve("storage").resolve("sst-000001.sinked.sst");
        assertTrue(Files.exists(newFile));
        assertFalse(Files.exists(sinkedFile));

        RecordingSinkManager recoveringSink = new RecordingSinkManager(77);
        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg, recoveringSink);
        dir2.init();
        try {
            assertEquals(0, recoveringSink.prepareCalls);
            assertEquals(1, recoveringSink.commitCalls);
            assertArrayEquals("v1".getBytes(), dir2.get("k1".getBytes()).orElse(null));

            BucketStateSnapshot snap = dir2.stateSnapshot();
            assertEquals(0, snap.newSSTCount());
            assertEquals(1, snap.sinkedSSTCount());
            assertEquals(77L, snap.lastSinkedSnapshotId());
            assertFalse(Files.exists(newFile));
            assertTrue(Files.exists(sinkedFile));
        } finally {
            dir2.close();
        }

        RecordingSinkManager alreadyRecoveredSink = new RecordingSinkManager(88);
        PMSBucketDirectorImpl dir3 = new PMSBucketDirectorImpl(cfg, alreadyRecoveredSink);
        dir3.init();
        try {
            assertEquals(0, alreadyRecoveredSink.prepareCalls);
            assertEquals(0, alreadyRecoveredSink.commitCalls);
            assertEquals(77L, dir3.stateSnapshot().lastSinkedSnapshotId());
        } finally {
            dir3.close();
        }
    }

    // ── Recovery ──

    @Test
    void recoverFromWALAfterRestart() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        // Write data, close
        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.put("k2".getBytes(), "v2".getBytes());
        dir1.delete("k1".getBytes());
        dir1.close();

        // Re-open and verify recovery
        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg);
        dir2.init();
        try {
            assertFalse(dir2.get("k1".getBytes()).isPresent(), "k1 was deleted");
            assertTrue(dir2.get("k2".getBytes()).isPresent(), "k2 should be recovered");
            assertArrayEquals("v2".getBytes(), dir2.get("k2".getBytes()).orElse(null));
            assertEquals(3L, dir2.stateSnapshot().lastAssignedSequenceId());
        } finally {
            dir2.close();
        }
    }

    @Test
    void recoverWithFreezeAndOverwrite() throws IOException {
        PMSConfig cfg = config(1_000_000, 256);

        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.put("k1".getBytes(), "v2".getBytes());
        dir1.close();

        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg);
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

        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg);
        dir1.init();
        dir1.put("k1".getBytes(), "v1".getBytes());
        dir1.freezeCurMemTable();
        dir1.flushImmutableMemTable();
        dir1.put("k2".getBytes(), "v2".getBytes());
        dir1.close();

        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg);
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
        PMSBucketDirectorImpl dir = new PMSBucketDirectorImpl(config(10, 256));
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
    void fullLifecycleWriteFreezeOverwriteRecover() throws IOException {
        PMSConfig cfg = config(3, 256);

        PMSBucketDirectorImpl dir1 = new PMSBucketDirectorImpl(cfg);
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
        PMSBucketDirectorImpl dir2 = new PMSBucketDirectorImpl(cfg);
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

    private static class RecordingSinkManager implements SinkManager {
        private final long snapshotId;
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
            List<Long> sstIds = batch.ssts().stream().map(SSTMeta::fileId).toList();
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
}
