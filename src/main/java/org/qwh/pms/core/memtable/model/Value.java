package org.qwh.pms.core.memtable.model;

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
}
