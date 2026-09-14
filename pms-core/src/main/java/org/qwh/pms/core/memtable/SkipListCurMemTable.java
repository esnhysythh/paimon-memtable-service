package org.qwh.pms.core.memtable;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListCurMemTable implements CurMemTable {

    private final MemTableConfig config;
    private final ConcurrentSkipListMap<Key, Value> map;
    // The director serializes put/freeze, including these boundary metadata updates.
    // Volatile fields also allow diagnostic reads without taking the write lock; a diagnostic
    // sample is not an atomic snapshot. estimatedSize is a freeze heuristic, not exact accounting.
    // The concurrent map supports readers, but does not replace the write-boundary protocol.
    private volatile long estimatedSize;
    private volatile int estimatedEntryCount;
    private volatile long minSequenceId;
    private volatile long maxSequenceId;
    private volatile long oldestWriteAtMillis;
    private volatile boolean frozen;

    public SkipListCurMemTable(MemTableConfig config) {
        this.config = config;
        this.map = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
        this.estimatedEntryCount = 0;
        this.minSequenceId = 0;
        this.maxSequenceId = 0;
        this.oldestWriteAtMillis = 0;
        this.frozen = false;
    }

    @Override
    public void put(Key key, Value value) {
        if (frozen) {
            throw new IllegalStateException("frozen CurMemTable cannot accept writes");
        }
        // PMSBucketDirectorImpl serializes put/freeze with its write boundary lock. The
        // volatile field keeps diagnostic reads visible without adding an AtomicLong or
        // another lock to every write. Concurrent direct puts remain safe for map content;
        // exact boundary metadata requires the documented director serialization.
        Value old = map.put(key, value);
        if (oldestWriteAtMillis == 0) {
            oldestWriteAtMillis = System.currentTimeMillis();
        }
        updateEstimatedSize(key, value, old);
        updateSequenceBounds(value.sequenceId());
        if (old == null) {
            estimatedEntryCount++;
        }
    }

    @Override
    public Value get(Key key) {
        return map.get(key);
    }

    @Override
    public ImmutableMemTable freeze() {
        // The director serializes put/freeze and publishes a newly allocated active table.
        // Keep this backing map intact so a query holding the previous MemTableState can
        // safely finish without taking the write boundary lock.
        if (frozen) {
            throw new IllegalStateException("CurMemTable is already frozen");
        }
        frozen = true;
        return new SkipListImmutableMemTable(
            map,
            estimatedSize,
            estimatedEntryCount,
            minSequenceId,
            maxSequenceId,
            oldestWriteAtMillis
        );
    }

    /**
     * Check whether this MemTable has reached either the entry count or size threshold
     * and should be frozen.
     */
    public boolean shouldFreeze() {
        return estimatedEntryCount >= config.maxEntries() || estimatedSize >= config.maxSizeBytes();
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

    @Override
    public Iterator<Entry> iterator() {
        // Direct iterator over entrySet, no Stream — avoids boxing overhead and
        // is compatible with future off-heap implementations (e.g. OakMap zero-copy).
        return new EntryIterator(map.entrySet().iterator());
    }

    @Override
    public Iterator<Entry> iterator(Key startInclusive, Optional<Key> endExclusive) {
        NavigableMap<Key, Value> range = endExclusive
            .map(end -> map.subMap(startInclusive, true, end, false))
            .orElseGet(() -> map.tailMap(startInclusive, true));
        return new EntryIterator(range.entrySet().iterator());
    }

    private void updateEstimatedSize(Key key, Value value, Value old) {
        int delta = 8 + 4 + key.size() + 4 + value.size();
        if (old != null) {
            delta -= 8 + 4 + key.size() + 4 + old.size();
        }
        // Rough estimate of ConcurrentSkipListMap node overhead: object header (16B) +
        // key/value refs (16B) + forward pointers (24B) + alignment ≈ 64B.
        // Actual overhead varies by JVM and can deviate ±50% from this estimate.
        // This is acceptable: estimatedSize is only used for freeze-threshold heuristics,
        // not for precise memory accounting.
        if (old == null) {
            delta += 64;
        }
        estimatedSize += delta;
    }

    private void updateSequenceBounds(long sequenceId) {
        if (sequenceId <= 0) {
            return;
        }
        // 0 is the uninitialized sentinel; real sequenceIds start from 1.
        // Callers must serialize write-boundary updates with freeze.
        if (minSequenceId == 0 || sequenceId < minSequenceId) {
            minSequenceId = sequenceId;
        }
        if (sequenceId > maxSequenceId) {
            maxSequenceId = sequenceId;
        }
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
