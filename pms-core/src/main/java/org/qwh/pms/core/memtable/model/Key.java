package org.qwh.pms.core.memtable.model;

import java.util.Arrays;

/**
 * Byte array key with unsigned lexicographical comparison (aligned with Paimon primary key sort order).
 * <p>
 * <b>Immutability contract:</b> The internal byte[] is NOT defensively copied for performance.
 * Callers MUST NOT modify the array after passing it to this class — doing so will corrupt
 * the SkipList ordering invariant and cause data loss or infinite loops.
 */
public record Key(byte[] bytes) implements Comparable<Key> {

    public Key {
        if (bytes == null) {
            throw new NullPointerException("key bytes must not be null");
        }
    }

    @Override
    public int compareTo(Key other) {
        int len = Math.min(this.bytes.length, other.bytes.length);
        for (int i = 0; i < len; i++) {
            // Unsigned byte comparison to align with Paimon primary key sort order.
            // Java Byte.compare() is signed and would give wrong ordering for bytes >= 0x80.
            int cmp = (this.bytes[i] & 0xFF) - (other.bytes[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(this.bytes.length, other.bytes.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key other)) return false;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    public int size() {
        return bytes.length;
    }
}
