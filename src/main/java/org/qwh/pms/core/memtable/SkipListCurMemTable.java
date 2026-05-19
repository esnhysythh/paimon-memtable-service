package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListCurMemTable implements CurMemTable {

    // volatile: freeze() swaps the map reference, other threads must see the new map immediately
    private volatile ConcurrentSkipListMap<Key, Value> map;
    // Not AtomicLong: estimatedSize is for flush threshold heuristics only, not precise accounting.
    // Concurrent updates may lose small deltas, but the 64B per-node overhead estimate already has
    // far larger error margin. Making it precise (e.g. AtomicLong) doesn't fix the real race
    // between "read size → decide freeze", so the added cost isn't justified.
    private volatile long estimatedSize;
    private volatile int entryCount;

    public SkipListCurMemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
        this.entryCount = 0;
    }

    @Override
    public void put(Key key, Value value) {
        Value old = map.put(key, value);
        updateEstimatedSize(key, value, old);
        if (old == null) {
            entryCount++;
        }
    }

    @Override
    public void delete(Key key) {
        Value old = map.put(key, Value.TOMBSTONE);
        updateEstimatedSize(key, Value.TOMBSTONE, old);
        if (old == null) {
            entryCount++;
        }
    }

    @Override
    public Value get(Key key) {
        return map.get(key);
    }

    @Override
    public ImmutableMemTable freeze() {
        // Capture current state, then swap in a fresh map — no data copied, only reference reassignment.
        // Race tolerance: a concurrent put that already read the old map reference may write
        // into the old map after we've handed it to the frozen instance. This is acceptable:
        // the stray entry is valid data that simply gets flushed with the frozen batch.
        // Same trade-off as RocksDB/LevelDB — strict consistency requires a flush coordinator
        // at the engine level, not locking the hot put path.
        ConcurrentSkipListMap<Key, Value> oldMap = this.map;
        long oldSize = this.estimatedSize;
        int oldCount = this.entryCount;

        this.map = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
        this.entryCount = 0;

        return new SkipListImmutableMemTable(oldMap, oldSize, oldCount);
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public int entryCount() {
        return entryCount;
    }

    @Override
    public Iterator<Entry> iterator() {
        // Direct iterator over entrySet, no Stream — avoids boxing overhead and
        // is compatible with future off-heap implementations (e.g. OakMap zero-copy).
        return new EntryIterator(map.entrySet().iterator());
    }

    private void updateEstimatedSize(Key key, Value value, Value old) {
        int delta = 4 + key.size() + 4 + value.size();
        if (old != null) {
            delta -= 4 + key.size() + 4 + old.size();
        }
        // Rough estimate of ConcurrentSkipListMap node overhead: object header (16B) +
        // key/value refs (16B) + forward pointers (24B) + alignment ≈ 64B.
        if (old == null) {
            delta += 64;
        }
        estimatedSize += delta;
    }

    private static class EntryIterator implements Iterator<Entry> {
        private final Iterator<Map.Entry<Key, Value>> inner;

        EntryIterator(Iterator<Map.Entry<Key, Value>> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Entry next() {
            Map.Entry<Key, Value> e = inner.next();
            return new Entry(e.getKey(), e.getValue());
        }
    }
}
