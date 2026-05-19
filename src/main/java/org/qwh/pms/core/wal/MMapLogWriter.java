package org.qwh.pms.core.wal;

import org.qwh.pms.core.wal.util.ByteBufferSupport;
import org.qwh.pms.core.wal.util.Closeables;
import org.qwh.pms.core.wal.util.Slice;
import org.qwh.pms.core.wal.util.SliceInput;
import org.qwh.pms.core.wal.util.SliceOutput;
import org.qwh.pms.core.wal.util.Slices;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.qwh.pms.core.wal.LogConstants.BLOCK_SIZE;
import static org.qwh.pms.core.wal.LogConstants.HEADER_SIZE;
import static org.qwh.pms.core.wal.Logs.getChunkChecksum;

public class MMapLogWriter
        implements LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;

    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private MappedByteBuffer mappedByteBuffer;
    private long fileOffset;
    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        if (file == null) throw new NullPointerException("file is null");
        if (fileNumber < 0) throw new IllegalArgumentException("fileNumber is negative");
        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closed.set(true);

        destroyMappedByteBuffer();

        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    public synchronized void delete()
            throws IOException
    {
        close();

        // try to delete the file
        file.delete();
    }

    private void destroyMappedByteBuffer()
    {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    @Override
    public File getFile()
    {
        return file;
    }

    @Override
    public long getFileNumber()
    {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public synchronized void addRecord(Slice record, boolean force)
            throws IOException
    {
        if (closed.get()) throw new IllegalStateException("Log has been closed");

        SliceInput sliceInput = record.input();

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            if (bytesRemainingInBlock < 0) throw new IllegalStateException();

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    ensureCapacity(bytesRemainingInBlock);
                    mappedByteBuffer.put(new byte[bytesRemainingInBlock]);
                }
                blockOffset = 0;
                bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
            if (bytesAvailableInBlock < 0) throw new IllegalStateException();

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            if (sliceInput.available() > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            }
            else {
                end = true;
                fragmentLength = sliceInput.available();
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            }
            else if (begin) {
                type = LogChunkType.FIRST;
            }
            else if (end) {
                type = LogChunkType.LAST;
            }
            else {
                type = LogChunkType.MIDDLE;
            }

            // write the chunk
            writeChunk(type, sliceInput.readBytes(fragmentLength));

            // we are no longer on the first chunk
            begin = false;
        } while (sliceInput.isReadable());

        if (force) {
            mappedByteBuffer.force();
        }
    }

    private void writeChunk(LogChunkType type, Slice slice)
            throws IOException
    {
        if (slice.length() > 0xffff) throw new IllegalArgumentException("length " + slice.length() + " is larger than two bytes");
        if (blockOffset + HEADER_SIZE > BLOCK_SIZE) throw new IllegalArgumentException();

        // create header
        Slice header = newLogRecordHeader(type, slice);

        // write the header and the payload
        ensureCapacity(header.length() + slice.length());
        header.getBytes(0, mappedByteBuffer);
        slice.getBytes(0, mappedByteBuffer);

        blockOffset += HEADER_SIZE + slice.length();
    }

    private void ensureCapacity(int bytes)
            throws IOException
    {
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();

            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }
    }

    private void unmap()
    {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }

    private static Slice newLogRecordHeader(LogChunkType type, Slice slice)
    {
        int crc = getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), slice.length());

        // Format the header
        Slice header = Slices.allocate(HEADER_SIZE);
        SliceOutput sliceOutput = header.output();
        sliceOutput.writeInt(crc);
        sliceOutput.writeByte((byte) (slice.length() & 0xff));
        sliceOutput.writeByte((byte) (slice.length() >>> 8));
        sliceOutput.writeByte((byte) (type.getPersistentId()));

        return header;
    }
}
