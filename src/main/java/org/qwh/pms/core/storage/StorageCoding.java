package org.qwh.pms.core.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

final class StorageCoding {
    private StorageCoding() {
    }

    static void writeByte(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
    }

    static void writeIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    static void writeLongLE(ByteArrayOutputStream out, long value) {
        out.write((int) (value & 0xFF));
        out.write((int) ((value >>> 8) & 0xFF));
        out.write((int) ((value >>> 16) & 0xFF));
        out.write((int) ((value >>> 24) & 0xFF));
        out.write((int) ((value >>> 32) & 0xFF));
        out.write((int) ((value >>> 40) & 0xFF));
        out.write((int) ((value >>> 48) & 0xFF));
        out.write((int) ((value >>> 56) & 0xFF));
    }

    static int readIntLE(byte[] data, int offset) {
        return (data[offset] & 0xFF)
            | ((data[offset + 1] & 0xFF) << 8)
            | ((data[offset + 2] & 0xFF) << 16)
            | ((data[offset + 3] & 0xFF) << 24);
    }

    static long readLongLE(byte[] data, int offset) {
        return ((long) data[offset] & 0xFF)
            | (((long) data[offset + 1] & 0xFF) << 8)
            | (((long) data[offset + 2] & 0xFF) << 16)
            | (((long) data[offset + 3] & 0xFF) << 24)
            | (((long) data[offset + 4] & 0xFF) << 32)
            | (((long) data[offset + 5] & 0xFF) << 40)
            | (((long) data[offset + 6] & 0xFF) << 48)
            | (((long) data[offset + 7] & 0xFF) << 56);
    }

    static void writeVarInt(ByteArrayOutputStream out, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("varint value must be non-negative");
        }
        while ((value & ~0x7F) != 0) {
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write(value);
    }

    static void writeVarLong(ByteArrayOutputStream out, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("varlong value must be non-negative");
        }
        while ((value & ~0x7FL) != 0) {
            out.write((int) (value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write((int) value);
    }

    static int readVarInt(ByteArrayInputStream in) {
        long value = readVarLong(in);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("varint too large: " + value);
        }
        return (int) value;
    }

    static long readVarLong(ByteArrayInputStream in) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int b = in.read();
            if (b < 0) {
                throw new IllegalArgumentException("truncated varint");
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IllegalArgumentException("varint too long");
    }

    static byte[] readExact(InputStream in, int length) throws IOException {
        byte[] data = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = in.read(data, offset, length - offset);
            if (read < 0) {
                throw new EOFException("expected " + length + " bytes, got " + offset);
            }
            offset += read;
        }
        return data;
    }

    static byte[] encodeHandle(BlockHandle handle) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(BlockHandle.MAX_ENCODED_LENGTH);
        writeVarLong(out, handle.offset());
        writeVarLong(out, handle.size());
        while (out.size() < BlockHandle.MAX_ENCODED_LENGTH) {
            out.write(0);
        }
        return out.toByteArray();
    }

    static BlockHandle decodeHandle(byte[] data, int offset) {
        byte[] slice = new byte[BlockHandle.MAX_ENCODED_LENGTH];
        System.arraycopy(data, offset, slice, 0, slice.length);
        ByteArrayInputStream in = new ByteArrayInputStream(slice);
        long handleOffset = readVarLong(in);
        long handleSize = readVarLong(in);
        return new BlockHandle(handleOffset, handleSize);
    }
}
