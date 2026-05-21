package org.qwh.pms.core.bucket;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.memtable.CurMemTable;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.wal.ReplayCallback;
import org.qwh.pms.core.wal.WALManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PMSBucketDirectorImpl implements PMSBucketDirector {

    private static final Logger LOG = LoggerFactory.getLogger(PMSBucketDirectorImpl.class);

    private final PMSConfig config;
    private final MemTableConfig memTableConfig;
    private final WALManagerImpl walManager;

    private volatile CurMemTable curMemTable;
    private volatile List<ImmutableMemTable> immutableMemTables = new ArrayList<>();

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object writeMutex = new Object();
    private volatile boolean closed = false;

    public PMSBucketDirectorImpl(PMSConfig config) {
        this.config = config;
        this.memTableConfig = config.memtable();
        this.walManager = new WALManagerImpl(config);
        this.curMemTable = new SkipListCurMemTable(memTableConfig);
    }

    public void init() throws IOException {
        walManager.init();
        recoverFromWAL();
        LOG.info("PMSBucketDirector initialized, curMemTable entries={}", curMemTable.estimatedEntryCount());
    }

    private void recoverFromWAL() {
        // V1: no Paimon integration, use highWatermark=0 to replay all records
        CollectingReplayCallback cb = new CollectingReplayCallback();
        walManager.replay(cb, 0);

        for (var record : cb.dataRecords) {
            Key key = new Key(record.key);
            Value value = record.value != null ? new Value(record.value, record.sequenceId) : Value.tombstone(record.sequenceId);
            curMemTable.put(key, value);
        }
        LOG.info("Recovered {} data records from WAL", cb.dataRecords.size());
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null; use delete(key) for tombstones or byte[0] for empty values");
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                long sequenceId = walManager.appendDataRecord(key, value);
                curMemTable.put(new Key(key), new Value(value, sequenceId));
                maybeFreezeLocked();
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void delete(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                long sequenceId = walManager.appendDataRecord(key, null);
                curMemTable.put(new Key(key), Value.tombstone(sequenceId));
                maybeFreezeLocked();
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            Key k = new Key(key);

            // Layer 1: curMemTable
            Value v = curMemTable.get(k);
            if (v != null) {
                if (v.isTombstone()) return Optional.empty();
                return Optional.of(v.bytes());
            }

            // Layer 2: immutableMemTables (reverse order, newest first)
            List<ImmutableMemTable> immutables = immutableMemTables;
            for (int i = immutables.size() - 1; i >= 0; i--) {
                v = immutables.get(i).get(k);
                if (v != null) {
                    if (v.isTombstone()) return Optional.empty();
                    return Optional.of(v.bytes());
                }
            }

            return Optional.empty();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void freezeCurMemTable() {
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                doFreezeLocked();
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public BucketStateSnapshot stateSnapshot() {
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            CurMemTable cur = curMemTable;
            List<ImmutableMemTable> immutables = immutableMemTables;

            long immTotalBytes = 0;
            long immMinSequenceId = 0;
            long immMaxSequenceId = 0;
            for (ImmutableMemTable im : immutables) {
                immTotalBytes += im.estimatedSize();
                if (im.minSequenceId() > 0 && (immMinSequenceId == 0 || im.minSequenceId() < immMinSequenceId)) {
                    immMinSequenceId = im.minSequenceId();
                }
                immMaxSequenceId = Math.max(immMaxSequenceId, im.maxSequenceId());
            }

            return new BucketStateSnapshot(
                cur.estimatedEntryCount(),
                cur.estimatedSize(),
                immutables.size(),
                immTotalBytes,
                walManager.lastSequenceId(),
                cur.minSequenceId(),
                cur.maxSequenceId(),
                immMinSequenceId,
                immMaxSequenceId,
                0L
            );
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            walManager.close();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    // ── Internal ──

    private void maybeFreezeLocked() {
        if (curMemTable.shouldFreeze()) {
            doFreezeLocked();
        }
    }

    private void doFreezeLocked() {
        if (curMemTable.estimatedEntryCount() == 0) {
            return;
        }
        ImmutableMemTable frozen = curMemTable.freeze();
        List<ImmutableMemTable> newList = new ArrayList<>(immutableMemTables);
        newList.add(frozen);
        immutableMemTables = newList;
        LOG.debug("Freeze: immutable count={}", newList.size());
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("PMSBucketDirector is closed");
    }

    // ── Recovery callback ──

    private static class CollectingReplayCallback implements ReplayCallback {
        final List<DataRecord> dataRecords = new ArrayList<>();

        @Override
        public void onDataRecord(byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(0, key, value));
        }

        @Override
        public void onDataRecord(long sequenceId, byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(sequenceId, key, value));
        }

        @Override
        public void onSinkPrepare(byte[] commitMessage) {
            // Not needed for basic recovery
        }

        @Override
        public void onSinkSuccess(long snapshotId) {
            // Not needed for basic recovery
        }
    }

    private record DataRecord(long sequenceId, byte[] key, byte[] value) {}
}
