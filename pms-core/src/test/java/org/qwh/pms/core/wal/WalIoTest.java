package org.qwh.pms.core.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.qwh.pms.core.wal.util.Slices;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.qwh.pms.core.wal.LogConstants.BLOCK_SIZE;

class WalIoTest {
    @TempDir Path dir;

    @Test
    void shortWritesAndReadsPreserveHeaderBodyPaddingAndFragmentOffsets() throws IOException {
        Path path = dir.resolve("log");
        FaultyChannel output = new FaultyChannel(FileChannel.open(path,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        output.maxWrite = 3;
        FileChannelLogWriter writer = new FileChannelLogWriter(path.toFile(), 1, output);
        byte[] first = new byte[BLOCK_SIZE - 7 - 3];
        byte[] second = new byte[2 * BLOCK_SIZE + 100];
        Arrays.fill(first, (byte) 11);
        Arrays.fill(second, (byte) 22);
        writer.addRecord(Slices.wrappedBuffer(first), false);
        writer.addRecord(Slices.wrappedBuffer(second), false);
        writer.close();
        try (FaultyChannel input = new FaultyChannel(FileChannel.open(path))) {
            input.maxRead = 17;
            LogReader reader = new LogReader(input, false);
            assertArrayEquals(first, reader.readRecord().getBytes());
            assertEquals(0, reader.getLastRecordOffset());
            assertEquals(BLOCK_SIZE - 3, reader.getLastRecordEnd());
            assertArrayEquals(second, reader.readRecord().getBytes());
            assertEquals(BLOCK_SIZE, reader.getLastRecordOffset());
            assertEquals(Files.size(path), reader.getLastRecordEnd());
            assertNull(reader.readRecord());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10, 32768})
    void readFailureIsNeverTreatedAsTailOrEof(int failAfter) throws IOException {
        Path path = dir.resolve("log");
        LogWriter writer = new FileChannelLogWriter(path.toFile(), 1);
        writer.addRecord(Slices.wrappedBuffer(new byte[100_000]), false);
        writer.close();
        try (FaultyChannel input = new FaultyChannel(FileChannel.open(path))) {
            input.failReadAfter = failAfter;
            LogReader reader = new LogReader(input, true);
            IOException error = assertThrows(IOException.class, reader::readRecord);
            assertTrue(error.getMessage().contains("IO_ERROR"));
            assertEquals("injected read failure", error.getCause().getMessage());
            assertFalse(reader.hasDiscardedTail());
        }
    }

    @Test
    void closedChannelAndZeroProgressAreNotEof() throws IOException {
        Path path = Files.createFile(dir.resolve("log"));
        FileChannel closed = FileChannel.open(path);
        closed.close();
        IOException error = assertThrows(IOException.class, () -> new LogReader(closed, true).readRecord());
        assertInstanceOf(ClosedChannelException.class, error.getCause());
        try (FaultyChannel input = new FaultyChannel(FileChannel.open(path))) {
            input.maxRead = 0;
            assertThrows(IOException.class, () -> new LogReader(input, true).readRecord());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 5, 8, 40})
    void incompleteWritesThrowInsteadOfReturningSuccess(int failAfter) throws IOException {
        Path path = dir.resolve("log");
        FaultyChannel output = new FaultyChannel(FileChannel.open(path,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        output.failWriteAfter = failAfter;
        FileChannelLogWriter writer = new FileChannelLogWriter(path.toFile(), 1, output);
        try {
            assertThrows(IOException.class, () -> writer.addRecord(Slices.wrappedBuffer(new byte[100]), false));
            assertEquals(failAfter, Files.size(path));
        } finally {
            writer.close();
        }
    }

    @Test
    void forceFailureIsPropagatedAndChannelStillCloses() throws IOException {
        Path path = dir.resolve("log");
        FaultyChannel output = new FaultyChannel(FileChannel.open(path,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        FileChannelLogWriter writer = new FileChannelLogWriter(path.toFile(), 1, output);
        writer.addRecord(Slices.wrappedBuffer(new byte[]{1}), false);
        output.failForce = true;
        IOException error = assertThrows(IOException.class, writer::close);
        assertEquals("injected force failure", error.getMessage());
        assertFalse(output.delegate.isOpen());
        assertTrue(writer.isClosed());
        writer.close();
    }

    @Test
    void zeroWriteProgressThrows() throws IOException {
        Path path = dir.resolve("log");
        FaultyChannel output = new FaultyChannel(FileChannel.open(path,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        output.maxWrite = 0;
        FileChannelLogWriter writer = new FileChannelLogWriter(path.toFile(), 1, output);
        try {
            assertThrows(IOException.class, () -> writer.addRecord(Slices.wrappedBuffer(new byte[]{1}), false));
        } finally {
            writer.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void creatingWriterNeverOverwritesAnExistingFile(boolean mmap) throws IOException {
        Path path = dir.resolve("log");
        byte[] original = {1, 2, 3};
        Files.write(path, original);
        assertThrows(IOException.class, () -> {
            if (mmap) new MMapLogWriter(path.toFile(), 1);
            else new FileChannelLogWriter(path.toFile(), 1);
        });
        assertArrayEquals(original, Files.readAllBytes(path));
    }

    private static final class FaultyChannel extends FileChannel {
        final FileChannel delegate;
        int maxRead = Integer.MAX_VALUE;
        int maxWrite = Integer.MAX_VALUE;
        long failReadAfter = Long.MAX_VALUE;
        long failWriteAfter = Long.MAX_VALUE;
        long readBytes;
        long writtenBytes;
        boolean failForce;

        FaultyChannel(FileChannel delegate) { this.delegate = delegate; }

        @Override public int read(ByteBuffer dst) throws IOException {
            if (readBytes >= failReadAfter) throw new IOException("injected read failure");
            if (maxRead == 0) return 0;
            int limit = dst.limit();
            dst.limit(dst.position() + (int) Math.min(Math.min(dst.remaining(), maxRead), failReadAfter - readBytes));
            try {
                int count = delegate.read(dst);
                if (count > 0) readBytes += count;
                return count;
            } finally { dst.limit(limit); }
        }

        @Override public int write(ByteBuffer src) throws IOException {
            if (writtenBytes >= failWriteAfter) throw new IOException("injected write failure");
            if (maxWrite == 0) return 0;
            int limit = src.limit();
            src.limit(src.position() + (int) Math.min(Math.min(src.remaining(), maxWrite), failWriteAfter - writtenBytes));
            try {
                int count = delegate.write(src);
                writtenBytes += count;
                return count;
            } finally { src.limit(limit); }
        }

        @Override public void force(boolean meta) throws IOException {
            if (failForce) throw new IOException("injected force failure");
            delegate.force(meta);
        }
        @Override protected void implCloseChannel() throws IOException { delegate.close(); }
        @Override public long position() throws IOException { return delegate.position(); }
        @Override public FileChannel position(long p) throws IOException { delegate.position(p); return this; }
        @Override public long size() throws IOException { return delegate.size(); }
        @Override public FileChannel truncate(long size) throws IOException { delegate.truncate(size); return this; }
        @Override public int read(ByteBuffer dst, long p) throws IOException { return delegate.read(dst, p); }
        @Override public long read(ByteBuffer[] dst, int o, int n) throws IOException { return delegate.read(dst, o, n); }
        @Override public int write(ByteBuffer src, long p) throws IOException { return delegate.write(src, p); }
        @Override public long write(ByteBuffer[] src, int o, int n) throws IOException { return delegate.write(src, o, n); }
        @Override public long transferTo(long p, long n, WritableByteChannel c) throws IOException { return delegate.transferTo(p, n, c); }
        @Override public long transferFrom(ReadableByteChannel c, long p, long n) throws IOException { return delegate.transferFrom(c, p, n); }
        @Override public MappedByteBuffer map(MapMode m, long p, long n) throws IOException { return delegate.map(m, p, n); }
        @Override public FileLock lock(long p, long n, boolean s) throws IOException { return delegate.lock(p, n, s); }
        @Override public FileLock tryLock(long p, long n, boolean s) throws IOException { return delegate.tryLock(p, n, s); }
    }
}
