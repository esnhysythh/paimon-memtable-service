package org.qwh.pms.core.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.PaimonConfig;
import org.qwh.pms.core.config.WalConfig;
import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;
import org.qwh.pms.core.wal.util.Slices;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.qwh.pms.core.wal.LogConstants.BLOCK_SIZE;

class WALRecoveryTest {
    @TempDir Path dir;

    private WALManagerImpl manager() {
        return new WALManagerImpl(new PMSConfig(null, new WalConfig(dir.toString(), 1, false),
            null, null, null, new PaimonConfig("dummy", null)));
    }

    private Path path(int number) {
        return dir.resolve(String.format("wal-%06d.log", number));
    }

    private void writeWal(int number, long sequence, Slice... records) throws IOException {
        LogWriter writer = new FileChannelLogWriter(path(number).toFile(), number);
        try {
            writer.addRecord(header(sequence), true);
            for (Slice record : records) writer.addRecord(record, false);
        } finally {
            writer.close();
        }
    }

    private static Slice header(long sequence) {
        DynamicSliceOutput out = new DynamicSliceOutput(20);
        out.writeBytes(new byte[]{'P', 'M', 'S', 0});
        out.writeLong(0);
        out.writeLong(sequence);
        return out.slice();
    }

    private static Slice batch(long begin, byte[]... values) {
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        out.writeByte(0x42);
        out.writeLong(begin);
        out.writeInt(values.length);
        for (byte[] value : values) {
            out.writeInt(1);
            out.writeByte('k');
            out.writeInt(value == null ? -1 : value.length);
            if (value != null) out.writeBytes(value);
        }
        return out.slice();
    }

    private void cut(Path file, long size) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(size);
        }
    }

    private IOException fails(long boundary, String reason) throws IOException {
        List<String> before;
        try (var files = Files.list(dir)) {
            before = files.map(p -> p.getFileName().toString()).sorted().toList();
        }
        WALManagerImpl manager = manager();
        IOException error = assertThrows(IOException.class, () -> manager.recover(boundary, (k, v) -> {}));
        assertTrue(error.getMessage().contains(reason), error.toString());
        assertEquals(0, manager.lastSequenceId(), "failed validation must not publish an allocator");
        assertThrows(IllegalStateException.class, () -> manager.appendDataRecord(new byte[0], null));
        manager.close();
        try (var files = Files.list(dir)) {
            assertEquals(before, files.map(p -> p.getFileName().toString()).sorted().toList());
        }
        return error;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void completeChecksumFailureIsFatalEvenAtEndOfLastBlock(boolean last) throws IOException {
        Slice first = batch(1, new byte[]{1});
        writeWal(1, 0, first, batch(2, new byte[]{2}));
        byte[] bytes = Files.readAllBytes(path(1));
        int offset = last ? 27 + 7 + first.length() : 27;
        bytes[offset] ^= 1;
        Files.write(path(1), bytes);
        IOException error = fails(0, "CHECKSUM_MISMATCH");
        assertEquals("Cannot recover WAL " + path(1) + ": CHECKSUM_MISMATCH at offset " + offset
            + ": invalid chunk checksum", error.getMessage());
        assertArrayEquals(bytes, Files.readAllBytes(path(1)));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 20, 32768, 32771, 65536})
    void incompleteLastBatchIsRemovedBeforeNextWriterAndRestart(int tailBytes) throws IOException {
        Slice first = batch(1, new byte[]{1});
        long completeEnd = 27 + 7 + first.length();
        writeWal(1, 0, first, batch(2, new byte[100_000], null));
        cut(path(1), completeEnd + tailBytes);
        WALManagerImpl wal = manager();
        WALManagerImplTest.CollectingCallback cb = new WALManagerImplTest.CollectingCallback();
        wal.recover(0, cb);
        assertEquals(1, cb.dataRecords.size());
        assertEquals(completeEnd, Files.size(path(1)));
        assertEquals(2, wal.appendDataRecord(new byte[]{'k'}, null));
        wal.close();
        WALManagerImpl again = manager();
        WALManagerImplTest.CollectingCallback replay = new WALManagerImplTest.CollectingCallback();
        again.recover(0, replay);
        try {
            assertEquals(2, replay.dataRecords.size());
            assertNull(replay.dataRecords.get(1).value());
            assertEquals(3, again.appendDataRecord(new byte[]{'x'}, new byte[]{3}));
        } finally {
            again.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {32768, 65536})
    void completeFirstAndMiddleChunksWithoutLastAreTail(int size) throws IOException {
        writeWal(1, 0, batch(1, new byte[100_000]));
        cut(path(1), size);
        WALManagerImpl wal = manager();
        wal.recover(0, (k, v) -> fail("fragmented batch must be discarded as a whole"));
        assertEquals(27, Files.size(path(1)));
        wal.close();
    }

    @Test
    void incompleteOlderFileIsFatal() throws IOException {
        writeWal(1, 0, batch(1, new byte[20]));
        cut(path(1), Files.size(path(1)) - 1);
        writeWal(2, 1);
        fails(0, "INCOMPLETE_TAIL");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 6, 7, 15, 26})
    void emptyOrIncompleteFileHeaderIsFatal(int size) throws IOException {
        writeWal(1, 0);
        cut(path(1), size);
        IOException error = fails(0, "offset 0");
        assertTrue(error.getMessage().startsWith("Cannot recover WAL " + path(1) + ": "));
        assertEquals(error.getMessage().indexOf("offset "), error.getMessage().lastIndexOf("offset "));
    }

    @ParameterizedTest
    @ValueSource(strings = {"magic", "reserved", "negative-sequence", "extra-byte", "data-instead"})
    void invalidFileHeaderWithValidChecksumIsFatal(String failure) throws IOException {
        Slice payload = header(0);
        switch (failure) {
            case "magic" -> payload.setByte(0, 'X');
            case "reserved" -> payload.setLong(4, 1);
            case "negative-sequence" -> payload.setLong(12, -1);
            case "extra-byte" -> payload = Slices.wrappedBuffer(Arrays.copyOf(payload.getBytes(), 21));
            case "data-instead" -> payload = batch(1, new byte[10]);
        }
        LogWriter writer = new FileChannelLogWriter(path(1).toFile(), 1);
        writer.addRecord(payload, true);
        writer.close();
        IOException error = fails(0, "invalid PMS WAL header");
        assertEquals("Cannot recover WAL " + path(1) + ": INVALID_FORMAT at offset 0: invalid PMS WAL header",
            error.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"type", "key", "huge-key", "value", "count", "overflow", "duplicate", "trailing", "second-row"})
    void malformedCompleteBatchNeverDeliversPartialRows(String failure) throws IOException {
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        out.writeByte(failure.equals("type") ? 0x41 : 0x42);
        out.writeLong(failure.equals("overflow") ? Long.MAX_VALUE : failure.equals("duplicate") ? 1 : 2);
        out.writeInt(failure.equals("count") ? Integer.MAX_VALUE : failure.equals("second-row") ? 2 : 1);
        out.writeInt(failure.equals("key") ? -1 : failure.equals("huge-key") ? Integer.MAX_VALUE : 0);
        out.writeInt(failure.equals("value") ? -2 : -1);
        if (failure.equals("trailing")) out.writeByte(0);
        if (failure.equals("second-row")) { out.writeInt(-1); out.writeInt(-1); }
        Slice first = batch(1, new byte[]{1});
        writeWal(1, 0, first, out.slice());
        WALManagerImpl wal = manager();
        WALManagerImplTest.CollectingCallback cb = new WALManagerImplTest.CollectingCallback();
        IOException error = assertThrows(IOException.class, () -> wal.recover(0, cb));
        assertTrue(error.getMessage().startsWith("Cannot recover WAL " + path(1) + ": record offset "
            + (27 + 7 + first.length()) + ": INVALID_FORMAT:"), error.toString());
        assertEquals(1, cb.dataRecords.size(), "no row from malformed batch may reach callback");
        assertFalse(Files.exists(path(2)));
        wal.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 4, 9})
    void invalidChunkTypesAndFragmentOrderAreFatalEvenWithShortBody(int type) throws IOException {
        writeWal(1, 0);
        ByteBuffer chunk = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        chunk.putInt(123).putShort((short) 20).put((byte) type).put((byte) 1);
        Files.write(path(1), chunk.array(), StandardOpenOption.APPEND);
        fails(0, "INVALID_FORMAT");
    }

    @Test
    void impossibleLengthAtEofIsNotATornTail() throws IOException {
        writeWal(1, 0, batch(1, new byte[]{1}));
        byte[] bytes = Files.readAllBytes(path(1));
        bytes[31] = (byte) 0xff;
        bytes[32] = (byte) 0xff;
        Files.write(path(1), bytes);
        fails(0, "chunk crosses block boundary");
    }

    @Test
    void zeroSuffixIsTrimmedButNonzeroDataAfterZerosIsFatal() throws IOException {
        writeWal(1, 0, batch(1, new byte[]{1}));
        long completeEnd = Files.size(path(1));
        Files.write(path(1), new byte[2 * BLOCK_SIZE], StandardOpenOption.APPEND);
        WALManagerImpl wal = manager();
        wal.recover(0, (k, v) -> {});
        wal.close();
        assertEquals(completeEnd, Files.size(path(1)));
        Files.write(path(2), new byte[BLOCK_SIZE], StandardOpenOption.APPEND);
        Files.write(path(2), new byte[]{1}, StandardOpenOption.APPEND);
        fails(0, "nonzero data");
    }

    @Test
    void zeroSuffixInOlderWalAndAllZeroFileAreFatal() throws IOException {
        writeWal(1, 0);
        Files.write(path(1), new byte[100], StandardOpenOption.APPEND);
        writeWal(2, 0);
        fails(0, "INCOMPLETE_TAIL");
        Files.delete(path(2));
        Files.write(path(1), new byte[100]);
        fails(0, "INCOMPLETE_TAIL");
    }

    @Test
    void fileGapAndHeaderWatermarkMismatchAreFatal() throws IOException {
        writeWal(1, 0, batch(1, new byte[]{1}));
        writeWal(3, 1);
        fails(0, "file gap");
        Files.move(path(3), path(2));
        Files.delete(path(2));
        writeWal(2, 2);
        fails(0, "inconsistent header sequence");
    }

    @Test
    void sequenceGapsAreAllowedButDurableBoundaryCannotBeInvented() throws IOException {
        writeWal(1, 0, batch(3, new byte[]{3}));
        fails(4, "Inconsistent WAL sequence");
        WALManagerImpl wal = manager();
        wal.recover(0, (k, v) -> {});
        assertEquals(4, wal.appendDataRecord(new byte[]{4}, null));
        wal.close();
    }

    @Test
    void missingPrefixMustBeCoveredByDurableState() throws IOException {
        writeWal(5, 10, batch(12, new byte[]{12}));
        fails(9, "inconsistent header sequence");
        WALManagerImpl wal = manager();
        wal.recover(10, (k, v) -> {});
        assertEquals(13, wal.appendDataRecord(new byte[]{13}, null));
        wal.close();
    }

    @Test
    void missingAllWalWithDurableHistoryFails() throws IOException {
        fails(1, "Inconsistent WAL sequence");
    }

    @ParameterizedTest
    @ValueSource(strings = {"wal-1.log", "wal-000000.log", "wal-bad.log", "wal-000001.tmp", "wal-9223372036854775807.log"})
    void malformedNamesAreNotSilentlyIgnored(String name) throws IOException {
        Files.createFile(dir.resolve(name));
        fails(0, "Invalid WAL file name");
    }

    @Test
    void fileRemovedDuringCallbackCannotBeIgnored() throws IOException {
        writeWal(1, 0, batch(1, new byte[]{1}));
        WALManagerImpl wal = manager();
        IOException error = assertThrows(IOException.class, () -> wal.recover(0, (k, v) -> {
            try { Files.delete(path(1)); } catch (IOException e) { throw new RuntimeException(e); }
        }));
        assertTrue(error.getMessage().contains("missing or replaced"));
        assertFalse(Files.exists(path(2)));
        wal.close();
    }

    @Test
    void prefixCleanupIncludesEmptyHeadersAndStopsAtFirstUnsafeFile() throws IOException {
        writeWal(1, 0);
        writeWal(2, 0, batch(1, new byte[]{1}));
        writeWal(3, 1);
        WALManagerImpl wal = manager();
        wal.recover(0, (k, v) -> {});
        wal.truncate(0);
        assertFalse(Files.exists(path(1)));
        assertTrue(Files.exists(path(2)));
        assertTrue(Files.exists(path(3)));
        wal.truncate(1);
        assertFalse(Files.exists(path(2)));
        assertFalse(Files.exists(path(3)));
        assertTrue(Files.exists(path(4)));
        wal.close();
        WALManagerImpl again = manager();
        again.recover(1, (k, v) -> fail());
        assertEquals(2, again.appendDataRecord(new byte[]{2}, null));
        again.close();
    }

    @Test
    void firstDeletionFailureStopsTheEntirePrefixAndRetries() throws IOException {
        writeWal(1, 0, batch(1, new byte[]{1}));
        writeWal(2, 1, batch(2, new byte[]{2}));
        WALManagerImpl wal = manager();
        wal.recover(0, (k, v) -> {});
        Files.move(path(1), dir.resolve("saved.log"));
        Files.createDirectory(path(1));
        Path blocker = Files.createFile(path(1).resolve("blocker"));
        wal.truncate(2);
        assertTrue(Files.exists(path(2)), "later eligible files must remain");
        Files.delete(blocker);
        Files.delete(path(1));
        Files.move(dir.resolve("saved.log"), path(1));
        wal.truncate(2);
        assertFalse(Files.exists(path(1)));
        assertFalse(Files.exists(path(2)));
        wal.close();
    }
}
