package org.qwh.pms.codec;

import java.io.ByteArrayOutputStream;

final class ByteUtils {

    static final int HEADER_SIZE = 12;
    static final int VERSION = 1;
    static final int FLAG_LARGE_ROW = 1;
    static final int FLAG_HAS_CHECKSUM = 1 << 1;

    private ByteUtils() {}

    static int u8(byte[] bytes, int offset) {
        return bytes[offset] & 0xFF;
    }

    static int u16le(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8);
    }

    static int i32le(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | (bytes[offset + 3] << 24);
    }

    static long i64le(byte[] bytes, int offset) {
        return ((long) bytes[offset] & 0xFF)
                | (((long) bytes[offset + 1] & 0xFF) << 8)
                | (((long) bytes[offset + 2] & 0xFF) << 16)
                | (((long) bytes[offset + 3] & 0xFF) << 24)
                | (((long) bytes[offset + 4] & 0xFF) << 32)
                | (((long) bytes[offset + 5] & 0xFF) << 40)
                | (((long) bytes[offset + 6] & 0xFF) << 48)
                | ((long) bytes[offset + 7] << 56);
    }

    static void writeU8(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
    }

    static void writeU16le(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    static void writeI16le(ByteArrayOutputStream out, short value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    static void writeI32le(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    static void writeI64le(ByteArrayOutputStream out, long value) {
        out.write((int) value & 0xFF);
        out.write((int) (value >>> 8) & 0xFF);
        out.write((int) (value >>> 16) & 0xFF);
        out.write((int) (value >>> 24) & 0xFF);
        out.write((int) (value >>> 32) & 0xFF);
        out.write((int) (value >>> 40) & 0xFF);
        out.write((int) (value >>> 48) & 0xFF);
        out.write((int) (value >>> 56) & 0xFF);
    }
}
