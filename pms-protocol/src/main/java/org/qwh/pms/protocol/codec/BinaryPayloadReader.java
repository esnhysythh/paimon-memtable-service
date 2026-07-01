package org.qwh.pms.protocol.codec;

import java.util.Arrays;
import java.util.Objects;

public final class BinaryPayloadReader {

    private final byte[] bytes;
    private int position;

    public BinaryPayloadReader(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes must not be null");
    }

    public int readInt() {
        requireRemaining(Integer.BYTES, "int32");
        int value =
                ((bytes[position] & 0xFF) << 24)
                        | ((bytes[position + 1] & 0xFF) << 16)
                        | ((bytes[position + 2] & 0xFF) << 8)
                        | (bytes[position + 3] & 0xFF);
        position += Integer.BYTES;
        return value;
    }

    public int readUnsignedByte() {
        requireRemaining(1, "byte");
        return bytes[position++] & 0xFF;
    }

    public byte[] readBytes(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("byte array length must not be negative: " + length);
        }
        requireRemaining(length, "byte array");
        byte[] result = Arrays.copyOfRange(bytes, position, position + length);
        position += length;
        return result;
    }

    public int remaining() {
        return bytes.length - position;
    }

    public void requireFullyRead() {
        if (remaining() != 0) {
            throw new IllegalArgumentException("Unexpected trailing bytes: " + remaining());
        }
    }

    private void requireRemaining(int length, String field) {
        if (remaining() < length) {
            throw new IllegalArgumentException(
                    "Not enough bytes for " + field + ": need " + length + ", remaining " + remaining());
        }
    }
}
