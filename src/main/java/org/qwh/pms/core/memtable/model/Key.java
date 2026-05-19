package org.qwh.pms.core.memtable.model;

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

    public int size() {
        return bytes.length;
    }
}
