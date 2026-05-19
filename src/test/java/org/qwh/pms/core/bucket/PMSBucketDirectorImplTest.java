package org.qwh.pms.core.bucket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.config.PMSConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PMSBucketDirectorImplTest {

    @TempDir
    Path tempDir;

    private PMSConfig config(int memtableMaxEntries, int memtableMaxSizeMb) {
        return new PMSConfig(
            memtableMaxEntries, memtableMaxSizeMb,
            tempDir.toString(), 256, false,
            10240, 100, 32, 4,
            30000, 8,
            4, 16,
            "", ""
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
            assertEquals(0, snap.curMemTableEntryCount());
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

    // ── End-to-end ──

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
}
