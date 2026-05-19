package org.qwh.pms.core.memtable.model;

public record Entry(Key key, Value value) {

    public boolean isTombstone() {
        return value.isTombstone();
    }

    public int estimatedSize() {
        return 4 + key.size() + 4 + value.size();
    }
}
