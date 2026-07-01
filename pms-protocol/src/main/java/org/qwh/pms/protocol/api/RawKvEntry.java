package org.qwh.pms.protocol.api;

import java.util.Objects;

public record RawKvEntry(byte[] key, byte[] row) {

    public RawKvEntry {
        Objects.requireNonNull(key, "key must not be null");
        if (key.length == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
    }

    public static RawKvEntry put(byte[] key, byte[] row) {
        Objects.requireNonNull(row, "row must not be null");
        return new RawKvEntry(key, row);
    }

    public static RawKvEntry delete(byte[] key) {
        return new RawKvEntry(key, null);
    }

    public boolean isDelete() {
        return row == null;
    }

    public byte[] value() {
        return row;
    }
}
