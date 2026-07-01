package org.qwh.pms.protocol.codec;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

public final class BinaryPayloadWriter {

    private final ByteArrayOutputStream out;

    public BinaryPayloadWriter() {
        this.out = new ByteArrayOutputStream();
    }

    public BinaryPayloadWriter(int initialSize) {
        this.out = new ByteArrayOutputStream(initialSize);
    }

    public BinaryPayloadWriter writeInt(int value) {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
        return this;
    }

    public BinaryPayloadWriter writeByte(int value) {
        out.write(value & 0xFF);
        return this;
    }

    public BinaryPayloadWriter writeBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        out.write(bytes, 0, bytes.length);
        return this;
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }
}
