package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

final class DataBlockBuilder {
    private final int restartInterval;
    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private final List<Integer> restartOffsets = new ArrayList<>();
    private Key lastKey;
    private int entriesSinceRestart;
    private int entryCount;

    DataBlockBuilder(int restartInterval) {
        this.restartInterval = Math.max(1, restartInterval);
        this.restartOffsets.add(0);
    }

    void add(Entry entry) {
        Key key = entry.key();
        Value value = entry.value();
        int shared = 0;
        if (lastKey != null && entriesSinceRestart < restartInterval) {
            shared = sharedBytes(lastKey.bytes(), key.bytes());
        } else if (lastKey != null) {
            restartOffsets.add(data.size());
            entriesSinceRestart = 0;
        }

        int unshared = key.bytes().length - shared;
        StorageCoding.writeVarInt(data, shared);
        StorageCoding.writeVarInt(data, unshared);
        StorageCoding.writeLongLE(data, value.sequenceId());
        StorageCoding.writeIntLE(data, value.isTombstone() ? SSTFormat.VALUE_LEN_DELETE : value.bytes().length);
        data.write(key.bytes(), shared, unshared);
        if (!value.isTombstone()) {
            data.writeBytes(value.bytes());
        }

        lastKey = key;
        entriesSinceRestart++;
        entryCount++;
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    int estimatedSize() {
        return data.size() + restartOffsets.size() * 4 + 4;
    }

    int entryCount() {
        return entryCount;
    }

    Key lastKey() {
        return lastKey;
    }

    byte[] finish() {
        for (Integer offset : restartOffsets) {
            StorageCoding.writeIntLE(data, offset);
        }
        StorageCoding.writeIntLE(data, restartOffsets.size());
        return data.toByteArray();
    }

    private static int sharedBytes(byte[] left, byte[] right) {
        int max = Math.min(left.length, right.length);
        int i = 0;
        while (i < max && left[i] == right[i]) {
            i++;
        }
        return i;
    }
}
