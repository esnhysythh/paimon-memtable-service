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
import java.util.Optional;

public class PMSBucketDirectorImpl implements PMSBucketDirector {

    private static final Logger LOG = LoggerFactory.getLogger(PMSBucketDirectorImpl.class);

    private final PMSConfig config;
    private final MemTableConfig memTableConfig;
    private final WALManagerImpl walManager;

    private volatile CurMemTable curMemTable;
    private volatile List<ImmutableMemTable> immutableMemTables = new ArrayList<>();

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
            Value value = record.value != null ? new Value(record.value) : Value.TOMBSTONE;
            curMemTable.put(key, value);
        }
        LOG.info("Recovered {} data records from WAL", cb.dataRecords.size());
    }

    @Override
    public void put(byte[] key, byte[] value) {
        ensureNotClosed();
        walManager.appendDataRecord(key, value);
        curMemTable.put(new Key(key), new Value(value));
        maybeFreeze();
    }

    @Override
    public void delete(byte[] key) {
        ensureNotClosed();
        walManager.appendDataRecord(key, null);
        curMemTable.delete(new Key(key));
        maybeFreeze();
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
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
    }

    @Override
    public void freezeCurMemTable() {
        ensureNotClosed();
        doFreeze();
    }

    @Override
    public BucketStateSnapshot stateSnapshot() {
        CurMemTable cur = curMemTable;
        List<ImmutableMemTable> immutables = immutableMemTables;

        long immTotalBytes = 0;
        for (ImmutableMemTable im : immutables) {
            immTotalBytes += im.estimatedSize();
        }

        return new BucketStateSnapshot(
            cur.estimatedEntryCount(),
            cur.estimatedSize(),
            immutables.size(),
            immTotalBytes,
            0L
        );
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        walManager.close();
    }

    // ── Internal ──

    private void maybeFreeze() {
        if (curMemTable.shouldFreeze()) {
            doFreeze();
        }
    }

    private synchronized void doFreeze() {
        // Secondary check: maybeFreeze() is lock-free, so multiple threads may see
        // shouldFreeze()==true and race into this method. After the first freeze,
        // curMemTable is replaced with an empty one — skip to avoid creating a
        // useless empty ImmutableMemTable.
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
            dataRecords.add(new DataRecord(key, value));
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

    private record DataRecord(byte[] key, byte[] value) {}
}
