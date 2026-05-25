package org.qwh.pms.core.memtable.model;

import java.util.Arrays;

/**
 * Byte array value with tombstone marker support (null bytes = deletion tombstone).
 * <p>
 * <b>Immutability contract:</b> The internal byte[] is NOT defensively copied for performance.
 * Callers MUST NOT modify the array after passing it to this class.
 * <p>
 * Empty values are represented by a zero-length byte array. A null byte array is reserved
 * exclusively for delete tombstones and must carry a positive sequence id.
 */
public record Value(byte[] bytes, long sequenceId) {

    public Value(byte[] bytes) {
        this(bytes, 0);
    }

    public Value {
        if (bytes == null && sequenceId <= 0) {
            throw new IllegalArgumentException("tombstone must carry a positive sequence id");
        }
    }

    public static Value tombstone(long sequenceId) {
        if (sequenceId <= 0) {
            throw new IllegalArgumentException("tombstone sequence id must be positive");
        }
        return new Value(null, sequenceId);
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
