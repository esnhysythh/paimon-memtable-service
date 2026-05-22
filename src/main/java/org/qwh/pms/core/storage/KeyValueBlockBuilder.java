package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

final class KeyValueBlockBuilder {
    private final int restartInterval;
    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private final List<Integer> restartOffsets = new ArrayList<>();
    private Key lastKey;
    private int entriesSinceRestart;
    private int entryCount;

    KeyValueBlockBuilder(int restartInterval) {
        this.restartInterval = Math.max(1, restartInterval);
        this.restartOffsets.add(0);
    }

    void add(Key key, byte[] value) {
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
        StorageCoding.writeVarInt(data, value.length);
        data.write(key.bytes(), shared, unshared);
        data.writeBytes(value);
        lastKey = key;
        entriesSinceRestart++;
        entryCount++;
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
