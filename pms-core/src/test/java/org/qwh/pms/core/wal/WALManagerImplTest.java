package org.qwh.pms.core.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.config.FlowControlConfig;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.PaimonConfig;
import org.qwh.pms.core.config.SinkConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.config.WalConfig;
import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    static class CollectingCallback implements ReplayCallback {
        final List<DataRecord> dataRecords = new ArrayList<>();

        @Override
        public void onDataRecord(byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(0, key, value));
        }

        @Override
        public void onDataRecord(long sequenceId, byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(sequenceId, key, value));
        }
    }

    record DataRecord(long sequenceId, byte[] key, byte[] value) {}

    private CollectingCallback writeCloseAndReplay(WALManagerImpl writer, int walFileSizeMb) throws IOException {
        writer.close();
        WALManagerImpl reader = new WALManagerImpl(config(walFileSizeMb));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb);
        reader.close();
        return cb;
    }

    @Test
    void appendAndReplayDataRecord() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        long sequenceId = wal.appendDataRecord("key1".getBytes(), "value1".getBytes());

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1L, sequenceId);
        assertEquals(1, cb.dataRecords.size());
        assertEquals(1L, cb.dataRecords.get(0).sequenceId());
        assertArrayEquals("key1".getBytes(), cb.dataRecords.get(0).key());
        assertArrayEquals("value1".getBytes(), cb.dataRecords.get(0).value());
    }

    @Test
    void appendDataRecordReturnsIncreasingSequenceIds() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        try {
            assertEquals(1L, wal.appendDataRecord("k1".getBytes(), "v1".getBytes()));
            assertEquals(2L, wal.appendDataRecord("k2".getBytes(), "v2".getBytes()));
            assertEquals(2L, wal.lastSequenceId());
        } finally {
            wal.close();
        }
    }

    @Test
    void appendDataRecordsWritesSingleBatchWithContiguousSequenceIds() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        long sequenceBegin = wal.appendDataRecords(List.of(
            new WALManager.DataWrite("k1".getBytes(), "v1".getBytes()),
            new WALManager.DataWrite("k2".getBytes(), null),
            new WALManager.DataWrite("k1".getBytes(), "v3".getBytes())
        ));

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1L, sequenceBegin);
        assertEquals(3L, cb.dataRecords.get(2).sequenceId());
        assertEquals(3L, cb.dataRecords.size());
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(0).key());
        assertArrayEquals("v1".getBytes(), cb.dataRecords.get(0).value());
        assertArrayEquals("k2".getBytes(), cb.dataRecords.get(1).key());
        assertNull(cb.dataRecords.get(1).value());
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(2).key());
        assertArrayEquals("v3".getBytes(), cb.dataRecords.get(2).value());
    }

    @Test
    void mmapWriterAppendAndReplayDataRecord() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256, true));
        wal.init();
        wal.appendDataRecord("key1".getBytes(), "value1".getBytes());

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.dataRecords.size());
        assertArrayEquals("key1".getBytes(), cb.dataRecords.get(0).key());
        assertArrayEquals("value1".getBytes(), cb.dataRecords.get(0).value());
    }

    @Test
    void appendAndReplayDelete() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("key1".getBytes(), null);

        CollectingCallback cb = writeCloseAndReplay(wal, 256);

        assertEquals(1, cb.dataRecords.size());
        assertNull(cb.dataRecords.get(0).value(), "value=null means Delete");
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
            assertEquals(i + 1L, cb.dataRecords.get(i).sequenceId());
            assertArrayEquals(("key" + i).getBytes(), cb.dataRecords.get(i).key());
            assertArrayEquals(("val" + i).getBytes(), cb.dataRecords.get(i).value());
        }
    }

    @Test
    void fileRolling() throws IOException {
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
    void truncateRemovesFilesCoveredByPersistedSequence() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(1));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());

        byte[] padding = new byte[1100 * 1024];
        Arrays.fill(padding, (byte) 'P');
        wal.appendDataRecord("pad".getBytes(), padding);

        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.truncate(2L);

        CollectingCallback cb = writeCloseAndReplay(wal, 1);

        boolean hasK1 = cb.dataRecords.stream().anyMatch(r -> Arrays.equals(r.key(), "k1".getBytes()));
        boolean hasK2 = cb.dataRecords.stream().anyMatch(r -> Arrays.equals(r.key(), "k2".getBytes()));
        assertFalse(hasK1, "k1 should have been truncated");
        assertTrue(hasK2, "k2 should remain after truncate");
    }

    @Test
    void truncateNeverDeletesCurrentFile() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());

        wal.truncate(200L);

        CollectingCallback cb = writeCloseAndReplay(wal, 256);
        assertEquals(1, cb.dataRecords.size());
    }

    @Test
    void initRecoversExistingFiles() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.close();

        WALManagerImpl recovered = new WALManagerImpl(config(256));
        recovered.init();
        CollectingCallback cb = new CollectingCallback();
        recovered.replay(cb);
        recovered.close();

        assertEquals(1, cb.dataRecords.size());
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(0).key());
    }

    @Test
    void sequenceIdContinuesAfterRestart() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        assertEquals(1L, wal.appendDataRecord("k1".getBytes(), "v1".getBytes()));
        assertEquals(2L, wal.appendDataRecord("k2".getBytes(), "v2".getBytes()));
        wal.close();

        WALManagerImpl recovered = new WALManagerImpl(config(256));
        recovered.init();
        try {
            assertEquals(2L, recovered.lastSequenceId());
            assertEquals(3L, recovered.appendDataRecord("k3".getBytes(), "v3".getBytes()));
        } finally {
            recovered.close();
        }
    }

    @Test
    void sequenceIdContinuesAfterTruncatingPreviousDataFiles() throws IOException {
        PMSConfig cfg = config(1);
        WALManagerImpl wal = new WALManagerImpl(cfg);
        wal.init();
        assertEquals(1L, wal.appendDataRecord("k1".getBytes(), "v1".getBytes()));

        byte[] padding = new byte[1100 * 1024];
        Arrays.fill(padding, (byte) 'P');
        wal.appendDataRecord("pad".getBytes(), padding);
        wal.truncate(2L);
        wal.close();

        WALManagerImpl recovered = new WALManagerImpl(cfg);
        recovered.init();
        try {
            assertEquals(2L, recovered.lastSequenceId());
            assertEquals(3L, recovered.appendDataRecord("k2".getBytes(), "v2".getBytes()));
        } finally {
            recovered.close();
        }
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
    }

    @Test
    void replayReturnsAllDataRecords() throws IOException {
        WALManagerImpl wal = new WALManagerImpl(config(256));
        wal.init();
        wal.appendDataRecord("k1".getBytes(), "v1".getBytes());
        wal.appendDataRecord("k2".getBytes(), "v2".getBytes());
        wal.close();

        WALManagerImpl reader = new WALManagerImpl(config(256));
        reader.init();
        CollectingCallback cb = new CollectingCallback();
        reader.replay(cb);
        reader.close();

        assertEquals(2, cb.dataRecords.size());
        assertArrayEquals("k1".getBytes(), cb.dataRecords.get(0).key());
        assertArrayEquals("k2".getBytes(), cb.dataRecords.get(1).key());
    }

    @Test
    void replayRejectsCorruptDataRecordLength() throws IOException {
        File file = tempDir.resolve("wal-000001.log").toFile();
        LogWriter writer = new FileChannelLogWriter(file, 1);
        try {
            writer.addRecord(fileHeader(), true);

            DynamicSliceOutput corrupt = new DynamicSliceOutput(12);
            corrupt.writeLong(1L);
            corrupt.writeInt(-1);
            writer.addRecord(corrupt.slice(), true);
        } finally {
            writer.close();
        }

        WALManagerImpl reader = new WALManagerImpl(config(256));
        reader.init();
        try {
            RuntimeException error = assertThrows(RuntimeException.class,
                () -> reader.replay(new CollectingCallback()));
            assertTrue(error.getMessage().contains("Corrupt WAL record"));
        } finally {
            reader.close();
        }
    }

    private static Slice fileHeader() {
        DynamicSliceOutput header = new DynamicSliceOutput(20);
        header.writeByte('P');
        header.writeByte('M');
        header.writeByte('S');
        header.writeByte(0);
        header.writeLong(0);
        header.writeLong(0);
        return header.slice();
    }
}
