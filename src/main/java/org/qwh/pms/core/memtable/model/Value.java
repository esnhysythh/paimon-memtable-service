package org.qwh.pms.core.memtable.model;

import java.util.Arrays;

/**
 * Byte array value with tombstone marker support (null bytes = deletion tombstone).
 * <p>
 * <b>Immutability contract:</b> The internal byte[] is NOT defensively copied for performance.
 * Callers MUST NOT modify the array after passing it to this class.
 */
public record Value(byte[] bytes) {

    public static final Value TOMBSTONE = new Value(null);

    public Value {
        // null bytes is allowed only for TOMBSTONE
    }

    public boolean isTombstone() {
        return bytes == null;
    }

    public int size() {
        return isTombstone() ? 0 : bytes.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Value other)) return false;
        if (this.bytes == null && other.bytes == null) return true;
        if (this.bytes == null || other.bytes == null) return false;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return bytes == null ? 0 : Arrays.hashCode(bytes);
    }
}
