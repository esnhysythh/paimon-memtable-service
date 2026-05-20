package org.qwh.pms.core.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.config.*;
import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WALManagerImplTest {

    @TempDir
    Path tempDir;

    private PMSConfig config(int walFileSizeMb) {
        return config(walFileSizeMb, false);
    }

    private PMSConfig config(int walFileSizeMb, boolean useMmap) {
        return new PMSConfig(
            new MemTableConfig(0, 0),
            new WalConfig(tempDir.toString(), walFileSizeMb, useMmap),
            new StorageConfig(0, 0, 0, 0),
            new SinkConfig(0, 0),
            new FlowControlConfig(0, 0),
            new PaimonConfig("dummy", null)
        );
    }

    // ── Collecting callback ──

    static class CollectingCallback implements ReplayCallback {
        final List<DataRecord> dataRecords = new ArrayList<>();
        final List<byte[]> sinkPrepares = new ArrayList<>();
        final List<Long> sinkSuccesses = new ArrayList<>();

        @Override
        public void onDataRecord(byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(key, value));
        }

        @Override
        public void onSinkPrepare(byte[] commitMessage) {
            sinkPrepares.add(commitMessage);
        }

        @Override
        public void onSinkSuccess(long snapshotId) {
            sinkSuccesses.add(snapshotId);
        }
    }

    record DataRecord(byte[] key, byte[] value) {}

    private static void assertDataEquals(DataRecord expected, DataRecord actual) {
        assertArrayEquals(expected.key, actual.key);
        if (expected.value == null) {
            assertNull(actual.value);
        } else {
            assertArrayEquals(expected.value, actual.value);
        }
    }

    /** Write data, close, then replay from a fresh WALManager (simulates restart). */
    private CollectingCallback writeCloseAndReplay(WALManagerImpl writer, int walFileSizeMb) throws IOException {
        writer.close();
        WALManagerImpl reader = new WALManagerImpl(config(walFileSizeMb));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb, Long.MAX_VALUE);
        reader.close();
        return cb;
    }

    // ── Tests ──

    @Test
    void appendAndReplayDataRecord() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("key1".getBytes(), "value1".getBytes());

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.dataRecords.size());
        assertDataEquals(new DataRecord("key1".getBytes(), "value1".getBytes()), cb.dataRecords.get(0));
    }

    @Test
    void mmapWriterAppendAndReplayDataRecord() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256, true));
        wal.init();
        wal.appendDataRecord("key1".getBytes(), "value1".getBytes());
        wal.close();

        WALManagerImpl reader = new WALManagerImpl(config(256, true));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb, Long.MAX_VALUE);
        reader.close();

        assertEquals(1, cb.dataRecords.size());
        assertDataEquals(new DataRecord("key1".getBytes(), "value1".getBytes()), cb.dataRecords.get(0));
    }

    @Test
    void appendAndReplayDelete() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("key1".getBytes(), null);

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.dataRecords.size());
        assertNull(cb.dataRecords.get(0).value, "value=null means Delete");
    }

    @Test
    void appendAndReplaySinkPrepare() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendSinkPrepare("commit-msg-1".getBytes());

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.sinkPrepares.size());
        assertArrayEquals("commit-msg-1".getBytes(), cb.sinkPrepares.get(0));
    }

    @Test
    void appendAndReplaySinkSuccess() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendSinkSuccess(42L);

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.sinkSuccesses.size());
        assertEquals(42L, cb.sinkSuccesses.get(0));
    }

    @Test
    void mixedRecordTypesReplayInOrder() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkPrepare("msg1".getBytes());
        wal.appendDataRecord("k2".getBytes(), null);
        wal.appendSinkSuccess(100L);

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(2, cb.dataRecords.size());
        assertEquals(1, cb.sinkPrepares.size());
        assertEquals(1, cb.sinkSuccesses.size());

        // Verify ordering
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(0).key);
        assertArrayEquals("msg1".getBytes(), cb.sinkPrepares.get(0));
        assertNull(cb.dataRecords.get(1).value);
        assertEquals(100L, cb.sinkSuccesses.get(0));
    }

    @Test
    void multipleDataRecordsReplayInOrder() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        for (int i = 0; i < 100; i++) {
            wal.appendDataRecord(("key" + i).getBytes(), ("val" + i).getBytes());
        }

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(100, cb.dataRecords.size());
        for (int i = 0; i < 100; i++) {
            assertArrayEquals(("key" + i).getBytes(), cb.dataRecords.get(i).key);
            assertArrayEquals(("val" + i).getBytes(), cb.dataRecords.get(i).value);
        }
    }

    @Test
    void fileRolling() throws IOException {
        // Use very small file size to trigger rolling
        WALManagerImpl wal = new WALManagerImpl(config(1));
        wal.init();

        byte[] largeValue = new byte[64 * 1024];
        Arrays.fill(largeValue, (byte) 'X');
        for (int i = 0; i < 20; i++) {
            wal.appendDataRecord(("key" + i).getBytes(), largeValue);
        }

        CollectingCallback cb = writeCloseAndReplay(wal, 1);

        assertEquals(20, cb.dataRecords.size());
    }

    @Test
    void truncateRemovesOldFiles() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(1));
        wal.init();

        // Step 1: Write k1 + SINK_SUCCESS(10) to file 1 (small, no roll yet)
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(10L);

        // Step 2: Fill file 1 past the 1MB limit to trigger a roll.
        byte[] padding = new byte[1100 * 1024];
        Arrays.fill(padding, (byte) 'P');
        wal.appendDataRecord("pad".getBytes(), padding);

        // Step 3: Now we're in file 2. Write k2 + SINK_SUCCESS(20).
        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.appendSinkSuccess(20L);

        // File 1: maxSnapshotId = 10 (in memory)
        // File 2: maxSnapshotId = 20 (current file)

        // Truncate files with maxSnapshotId <= 10
        wal.truncate(10L);

        CollectingCallback cb = writeCloseAndReplay(wal, 1);

        // k1 should be gone (file 1 was truncated), k2 should remain (file 2 kept)
        boolean hasK1 = cb.dataRecords.stream()
            .anyMatch(r -> Arrays.equals(r.key, "k1".getBytes()));
        boolean hasK2 = cb.dataRecords.stream()
            .anyMatch(r -> Arrays.equals(r.key, "k2".getBytes()));
        assertFalse(hasK1, "k1 should have been truncated");
        assertTrue(hasK2, "k2 should remain after truncate");
    }

    @Test
    void truncateNeverDeletesCurrentFile() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();

        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(100L);

        // truncate with a high safeSnapshotId
        wal.truncate(200L);

        // Current file should still exist
        CollectingCallback cb = writeCloseAndReplay(wal, 256);
        assertEquals(1, cb.dataRecords.size());
    }

    @Test
    void initRecoversExistingFiles() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(5L);
        wal.close();

        // Re-open on the same directory
        WALManagerImpl recovered = new WALManagerImpl(config(256));
        recovered.init();
        CollectingCallback cb = new CollectingCallback();
        recovered.replay(cb, Long.MAX_VALUE);
        recovered.close();

        assertEquals(1, cb.dataRecords.size());
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(0).key);
        assertEquals(5L, cb.sinkSuccesses.get(0));
    }

    @Test
    void appendAfterCloseThrows() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.close();

        assertThrows(IllegalStateException.class,
            () -> wal.appendDataRecord("k".getBytes(), "v".getBytes()));
    }

    @Test
    void emptyWalReplayReturnsNothing() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertTrue(cb.dataRecords.isEmpty());
        assertTrue(cb.sinkPrepares.isEmpty());
        assertTrue(cb.sinkSuccesses.isEmpty());
    }

    @Test
    void maxSnapshotIdSurvivesRestart() throws IOException {
        PMSConfig cfg = config(1);

        // Write data + SINK_SUCCESS(10) to file 1, then roll to file 2
        WALManagerImpl wal = new WALManagerImpl(cfg);
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(10L);
        byte[] padding = new byte[1100 * 1024];
        Arrays.fill(padding, (byte) 'P');
        wal.appendDataRecord("pad".getBytes(), padding);
        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.appendSinkSuccess(20L);
        wal.close();

        // Re-open and truncate with safeSnapshotId=10 — file 1 should be deleted
        WALManagerImpl recovered = new WALManagerImpl(cfg);
        recovered.init();
        recovered.truncate(10L);
        CollectingCallback cb = new CollectingCallback();
        recovered.replay(cb, Long.MAX_VALUE);
        recovered.close();

        boolean hasK1 = cb.dataRecords.stream()
            .anyMatch(r -> Arrays.equals(r.key, "k1".getBytes()));
        assertFalse(hasK1, "k1 should have been truncated after restart");
    }

    @Test
    void replayWithHighWatermarkSkipsOldData() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(5L);
        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.appendSinkSuccess(10L);
        wal.appendDataRecord("k3".getBytes(), "v3".getBytes());
        wal.close();

        WALManagerImpl reader = new WALManagerImpl(config(256));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb, 5L);
        reader.close();

        // Only DATA after SINK_SUCCESS(5) should be replayed: k2 and k3
        // k1 is before the highWatermark and should be skipped
        assertEquals(2, cb.dataRecords.size());
        assertArrayEquals("k2".getBytes(), cb.dataRecords.get(0).key);
        assertArrayEquals("k3".getBytes(), cb.dataRecords.get(1).key);
    }

    @Test
    void replayWithHighWatermarkScansFileWhenMaxSnapshotEqualsWatermark() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendSinkSuccess(5L);
        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.close();

        WALManagerImpl reader = new WALManagerImpl(config(256));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb, 5L);
        reader.close();

        assertEquals(1, cb.dataRecords.size());
        assertArrayEquals("k2".getBytes(), cb.dataRecords.get(0).key);
    }

    @Test
    void replayRejectsCorruptDataRecordLength() throws IOException {
        File file = tempDir.resolve("wal-000001.log").toFile();
        LogWriter writer = new FileChannelLogWriter(file, 1);
        try {
            writer.addRecord(fileHeader(), true);

            DynamicSliceOutput corrupt = new DynamicSliceOutput(5);
            corrupt.writeByte(WALManagerImpl.TYPE_DATA);
            corrupt.writeInt(-1);
            writer.addRecord(corrupt.slice(), true);
        } finally {
            writer.close();
        }

        WALManagerImpl reader = new WALManagerImpl(config(256));
        reader.init();
        try {
            RuntimeException error = assertThrows(RuntimeException.class,
                () -> reader.replay(new CollectingCallback(), Long.MAX_VALUE));
            assertTrue(error.getMessage().contains("Corrupt WAL record"));
        } finally {
            reader.close();
        }
    }

    private static Slice fileHeader() {
        DynamicSliceOutput header = new DynamicSliceOutput(12);
        header.writeByte('P');
        header.writeByte('M');
        header.writeByte('S');
        header.writeByte(0);
        header.writeLong(0);
        return header.slice();
    }
}
