package org.qwh.pms.core.wal;

import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;
import org.qwh.pms.core.wal.util.Slices;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static org.qwh.pms.core.wal.LogConstants.BLOCK_SIZE;
import static org.qwh.pms.core.wal.LogConstants.HEADER_SIZE;

/** Sequential, checksummed WAL reader. Corruption never resynchronizes to later records. */
public final class LogReader {
    private final FileChannel channel;
    private final boolean tolerateTail;
    private final ByteBuffer block = ByteBuffer.allocate(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    private final DynamicSliceOutput scratch = new DynamicSliceOutput(BLOCK_SIZE);
    private long blockOffset;
    private long nextBlockOffset;
    private long lastRecordOffset;
    private long lastRecordEnd;
    private boolean eof;
    private boolean finished;
    private boolean discardedTail;

    public LogReader(FileChannel channel, boolean tolerateTail) {
        this.channel = channel;
        this.tolerateTail = tolerateTail;
        block.limit(0);
    }

    public long getLastRecordOffset() {
        return lastRecordOffset;
    }

    public long getLastRecordEnd() {
        return lastRecordEnd;
    }

    public boolean hasDiscardedTail() {
        return discardedTail;
    }

    public Slice readRecord() throws IOException {
        if (finished) return null;
        scratch.reset();
        boolean fragmented = false;
        long recordOffset = -1;
        while (true) {
            if (block.remaining() < HEADER_SIZE) {
                if (block.hasRemaining()) {
                    if (block.limit() != BLOCK_SIZE) {
                        return incompleteTail(offset(), "truncated chunk header");
                    }
                    // Only the final 1..6 bytes of a full block can be alignment padding.
                    requireZeros();
                }
                if (!readBlock()) {
                    if (fragmented) return incompleteTail(recordOffset, "missing LAST chunk");
                    finished = true;
                    return null;
                }
                continue;
            }

            long chunkOffset = offset();
            int bytesInBlock = BLOCK_SIZE - block.position();
            int checksum = block.getInt();
            int length = Short.toUnsignedInt(block.getShort());
            int type = Byte.toUnsignedInt(block.get());
            if (type == 0 && length == 0 && checksum == 0) {
                // mmap preallocation is a zero suffix, never a hole followed by more data.
                requireZeros();
                while (readBlock()) requireZeros();
                return incompleteTail(chunkOffset, "zero-filled tail");
            }
            if (type < 1 || type > 4) {
                throw corruption(chunkOffset, "INVALID_FORMAT", "unknown chunk type " + type);
            }
            if (length > bytesInBlock - HEADER_SIZE) {
                throw corruption(chunkOffset, "INVALID_FORMAT", "chunk crosses block boundary");
            }
            boolean startsRecord = type == 1 || type == 2;
            if (startsRecord == fragmented) {
                throw corruption(chunkOffset, "INVALID_FORMAT", "invalid fragment order");
            }
            if (length > block.remaining()) {
                return incompleteTail(chunkOffset, "truncated chunk body");
            }
            int payloadOffset = block.position();
            if (checksum != Logs.getChunkChecksum(type, block.array(), payloadOffset, length)) {
                throw corruption(chunkOffset, "CHECKSUM_MISMATCH", "invalid chunk checksum");
            }
            block.position(payloadOffset + length);
            if (type == 1) {
                lastRecordOffset = chunkOffset;
                lastRecordEnd = offset();
                return Slices.wrappedBuffer(block.array()).slice(payloadOffset, length).copySlice();
            }
            if (type == 2) {
                recordOffset = chunkOffset;
                fragmented = true;
            }
            if (scratch.size() > Integer.MAX_VALUE - length) {
                throw corruption(chunkOffset, "INVALID_FORMAT", "logical record too large");
            }
            scratch.writeBytes(block.array(), payloadOffset, length);
            if (type == 4) {
                lastRecordOffset = recordOffset;
                lastRecordEnd = offset();
                return scratch.slice().copySlice();
            }
        }
    }

    private Slice incompleteTail(long offset, String reason) throws IOException {
        // A missing/partial first record is never a recoverable PMS file header.
        if (!tolerateTail || lastRecordEnd == 0) {
            throw corruption(offset, "INCOMPLETE_TAIL", reason);
        }
        discardedTail = true;
        finished = true;
        return null;
    }

    private void requireZeros() throws IOException {
        while (block.hasRemaining()) {
            long offset = offset();
            if (block.get() != 0) {
                throw corruption(offset, "INVALID_FORMAT", "nonzero data in padding/zero tail");
            }
        }
    }

    private boolean readBlock() throws IOException {
        if (eof) return false;
        block.clear();
        blockOffset = nextBlockOffset;
        try {
            while (block.hasRemaining()) {
                int read = channel.read(block);
                if (read < 0) {
                    eof = true;
                    break;
                }
                if (read == 0) throw new IOException("WAL read made no progress");
            }
        } catch (IOException e) {
            throw new IOException("IO_ERROR at offset " + offset() + ": " + e.getMessage(), e);
        }
        nextBlockOffset += block.position();
        block.flip();
        return block.hasRemaining();
    }

    private long offset() {
        return blockOffset + block.position();
    }

    private static IOException corruption(long offset, String kind, String reason) {
        return new IOException(kind + " at offset " + offset + ": " + reason);
    }
}
