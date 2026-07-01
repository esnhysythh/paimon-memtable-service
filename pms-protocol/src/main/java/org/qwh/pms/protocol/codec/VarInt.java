package org.qwh.pms.protocol.codec;

public final class VarInt {

    private VarInt() {}

    public static void writeUnsignedInt(BinaryPayloadWriter writer, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("uvarint32 value must not be negative: " + value);
        }
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            writer.writeByte((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        writer.writeByte(remaining);
    }

    public static int readUnsignedInt(BinaryPayloadReader reader) {
        int value = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int next = reader.readUnsignedByte();
            if (shift == 28 && (next & 0xF0) != 0) {
                throw new IllegalArgumentException("uvarint32 value exceeds int range");
            }
            value |= (next & 0x7F) << shift;
            if ((next & 0x80) == 0) {
                requireCanonical(value, shift / 7 + 1);
                return value;
            }
        }
        throw new IllegalArgumentException("uvarint32 is too long");
    }

    public static int encodedSize(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("uvarint32 value must not be negative: " + value);
        }
        int size = 1;
        int remaining = value >>> 7;
        while (remaining != 0) {
            size++;
            remaining >>>= 7;
        }
        return size;
    }

    private static void requireCanonical(int value, int encodedBytes) {
        if (encodedSize(value) != encodedBytes) {
            throw new IllegalArgumentException("non-canonical uvarint32 encoding");
        }
    }
}
