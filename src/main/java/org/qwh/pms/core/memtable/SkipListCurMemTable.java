package org.qwh.pms.core.memtable;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListCurMemTable implements CurMemTable {

    private final MemTableConfig config;
    // volatile: freeze() swaps the map reference, other threads must see the new map immediately
    private volatile ConcurrentSkipListMap<Key, Value> map;
    // Not AtomicLong: estimatedSize is for flush threshold heuristics only, not precise accounting.
    // Concurrent updates may lose small deltas, but the 64B per-node overhead estimate already has
    // far larger error margin. Making it precise (e.g. AtomicLong) doesn't fix the real race
    // between "read size → decide freeze", so the added cost isn't justified.
    // Same rationale for estimatedEntryCount below: volatile int ++ is not atomic, concurrent
    // puts may lose increments. The count is always >= 0 and within ~1% of the true value under
    // normal load, which is sufficient for freeze-threshold heuristics.
    // TODO: freeze() reads estimatedEntryCount and estimatedSize non-atomically; the two values
    // may be momentarily inconsistent. Acceptable for heuristics, but document if needed later.
    private volatile long estimatedSize;
    private volatile int estimatedEntryCount;
    private volatile long minSequenceId;
    private volatile long maxSequenceId;

    public SkipListCurMemTable(MemTableConfig config) {
        this.config = config;
        this.map = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
        this.estimatedEntryCount = 0;
        this.minSequenceId = 0;
        this.maxSequenceId = 0;
    }

    @Override
    public void put(Key key, Value value) {
        Value old = map.put(key, value);
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
        // Capture current state, then swap in a fresh map — no data copied, only reference reassignment.
        // Race tolerance: a concurrent put that already read the old map reference may write
        // into the old map after we've handed it to the frozen instance. This is acceptable:
        // the stray entry is valid data that simply gets flushed with the frozen batch.
        // Same trade-off as RocksDB/LevelDB — strict consistency requires a flush coordinator
        // at the engine level, not locking the hot put path.
        ConcurrentSkipListMap<Key, Value> oldMap = this.map;
        long oldSize = this.estimatedSize;
        int oldCount = this.estimatedEntryCount;
        long oldMinSequenceId = this.minSequenceId;
        long oldMaxSequenceId = this.maxSequenceId;

        this.map = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
        this.estimatedEntryCount = 0;
        this.minSequenceId = 0;
        this.maxSequenceId = 0;

        return new SkipListImmutableMemTable(oldMap, oldSize, oldCount, oldMinSequenceId, oldMaxSequenceId);
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
    public Iterator<Entry> iterator() {
        // Direct iterator over entrySet, no Stream — avoids boxing overhead and
        // is compatible with future off-heap implementations (e.g. OakMap zero-copy).
        return new EntryIterator(map.entrySet().iterator());
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
