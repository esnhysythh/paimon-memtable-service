package org.qwh.pms.core.memtable;

import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SkipListImmutableMemTable implements ImmutableMemTable {

    private final ConcurrentSkipListMap<Key, Value> map;
    private final long estimatedSize;
    private final int entryCount;
    private final AtomicLong refCount = new AtomicLong(0);

    SkipListImmutableMemTable(ConcurrentSkipListMap<Key, Value> map, long estimatedSize, int entryCount) {
        this.map = map;
        this.estimatedSize = estimatedSize;
        this.entryCount = entryCount;
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
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public int entryCount() {
        return entryCount;
    }

    @Override
    public void incrementRef() {
        refCount.incrementAndGet();
    }

    @Override
    public void decrementRef() {
        refCount.decrementAndGet();
    }

    @Override
    public long refCount() {
        return refCount.get();
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
