package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListImmutableMemTable implements ImmutableMemTable {

    private final ConcurrentSkipListMap<Key, Value> map;
    private final long estimatedSize;
    private final int estimatedEntryCount;
    private final long minSequenceId;
    private final long maxSequenceId;
    private final long oldestWriteAtMillis;

    SkipListImmutableMemTable(ConcurrentSkipListMap<Key, Value> map, long estimatedSize, int estimatedEntryCount,
                              long minSequenceId, long maxSequenceId, long oldestWriteAtMillis) {
        this.map = map;
        this.estimatedSize = estimatedSize;
        this.estimatedEntryCount = estimatedEntryCount;
        this.minSequenceId = minSequenceId;
        this.maxSequenceId = maxSequenceId;
        this.oldestWriteAtMillis = oldestWriteAtMillis;
    }

    @Override
    public Value get(Key key) {
        return map.get(key);
    }

    @Override
    public Iterator<Entry> iterator() {
        return new EntryIterator(map.entrySet().iterator());
    }

    @Override
    public Iterator<Entry> iterator(Key startInclusive, Optional<Key> endExclusive) {
        NavigableMap<Key, Value> range = endExclusive
            .map(end -> map.subMap(startInclusive, true, end, false))
            .orElseGet(() -> map.tailMap(startInclusive, true));
        return new EntryIterator(range.entrySet().iterator());
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public int estimatedEntryCount() {
        return estimatedEntryCount;
    }

    @Override
    public long minSequenceId() {
        return minSequenceId;
    }

    @Override
    public long maxSequenceId() {
        return maxSequenceId;
    }

    @Override
    public long oldestWriteAtMillis() {
        return oldestWriteAtMillis;
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
