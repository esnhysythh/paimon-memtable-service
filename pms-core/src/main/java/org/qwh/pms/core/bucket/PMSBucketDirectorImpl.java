package org.qwh.pms.core.bucket;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.memtable.CurMemTable;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.MockSinkManager;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.sink.SinkWalCodec;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.core.storage.SSTMeta;
import org.qwh.pms.core.storage.SSTState;
import org.qwh.pms.core.wal.ReplayCallback;
import org.qwh.pms.core.wal.WALManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PMSBucketDirectorImpl implements PMSBucketDirector {

    private static final Logger LOG = LoggerFactory.getLogger(PMSBucketDirectorImpl.class);

    private final MemTableConfig memTableConfig;
    private final WALManagerImpl walManager;
    private final FileLocalStorageManager storageManager;
    private final SinkManager sinkManager;

    private volatile CurMemTable curMemTable;
    private volatile List<ImmutableMemTable> immutableMemTables = new ArrayList<>();
    private volatile List<SSTMeta> newSSTs = new ArrayList<>();
    private volatile List<SSTMeta> sinkedSSTs = new ArrayList<>();
    private volatile long lastSinkedSnapshotId;

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object writeMutex = new Object();
    private volatile boolean closed = false;

    public PMSBucketDirectorImpl(PMSConfig config) {
        this.memTableConfig = config.memtable();
        this.walManager = new WALManagerImpl(config);
        this.storageManager = new FileLocalStorageManager(config.storage());
        this.sinkManager = new MockSinkManager();
        this.curMemTable = new SkipListCurMemTable(memTableConfig);
    }

    public void init() throws IOException {
        storageManager.init();
        walManager.init();
        RecoveryState recoveryState = recoverFromWAL();
        storageManager.applySinkedSSTIds(recoveryState.sinkedSSTIds());
        refreshSSTLists();
        lastSinkedSnapshotId = recoveryState.lastSinkedSnapshotId();
        LOG.info("PMSBucketDirector initialized, curMemTable entries={}", curMemTable.estimatedEntryCount());
    }

    private RecoveryState recoverFromWAL() {
        long lastFlushedSequenceId = storageManager.lastFlushedSequenceId();
        CollectingReplayCallback cb = new CollectingReplayCallback(lastFlushedSequenceId);
        walManager.replay(cb, 0);

        for (var record : cb.dataRecords) {
            Key key = new Key(record.key);
            Value value = record.value != null
                ? new Value(record.value, record.sequenceId)
                : Value.tombstone(record.sequenceId);
            curMemTable.put(key, value);
        }
        LOG.info(
            "Recovered {} data records from WAL, skippedFlushedRecords={}, lastFlushedSequenceId={}",
            cb.dataRecords.size(),
            cb.skippedFlushedRecords,
            lastFlushedSequenceId
        );
        return new RecoveryState(Set.copyOf(cb.sinkedSSTIds), cb.lastSinkedSnapshotId);
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

            Value v = curMemTable.get(k);
            if (v != null) {
                return bytesFromValue(v);
            }

            List<ImmutableMemTable> immutables = immutableMemTables;
            for (int i = immutables.size() - 1; i >= 0; i--) {
                v = immutables.get(i).get(k);
                if (v != null) {
                    return bytesFromValue(v);
                }
            }

            Optional<Value> newSSTValue = lookupSSTs(newSSTs, k);
            if (newSSTValue.isPresent()) {
                return bytesFromValue(newSSTValue.get());
            }
            return lookupSSTs(sinkedSSTs, k).flatMap(this::bytesFromValue);
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
    public void flushImmutableMemTable() {
        ImmutableMemTable toFlush;
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                if (immutableMemTables.isEmpty()) {
                    return;
                }
                toFlush = immutableMemTables.get(0);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }

        SSTMeta meta = storageManager.flushToSST(toFlush);
        storageManager.persistFlushedSequenceId(meta.maxSequenceId());

        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                List<ImmutableMemTable> immutableList = new ArrayList<>(immutableMemTables);
                immutableList.remove(toFlush);
                immutableMemTables = immutableList;

                List<SSTMeta> sstList = new ArrayList<>(newSSTs);
                sstList.add(meta);
                newSSTs = sstList;
                LOG.debug("Flush: immutable count={}, newSST count={}", immutableList.size(), sstList.size());
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void sinkToPaimon() {
        List<SSTMeta> toSink;
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                if (newSSTs.isEmpty()) {
                    return;
                }
                toSink = List.copyOf(newSSTs);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }

        SinkBatch batch = new SinkBatch(nextBatchId(toSink), toSink, minSequenceId(toSink), maxSequenceId(toSink));
        PreparedSinkCommit prepared = sinkManager.prepare(batch);
        walManager.appendSinkPrepare(SinkWalCodec.encodePrepare(prepared));
        SinkCommitResult result = sinkManager.commit(prepared);
        walManager.appendSinkSuccess(result.snapshotId(), SinkWalCodec.encodeSuccess(result));
        storageManager.markSinked(toSink);

        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                Set<Long> sinkedIds = new HashSet<>(result.sstIds());
                newSSTs = newSSTs.stream()
                    .filter(meta -> !sinkedIds.contains(meta.fileId()))
                    .toList();
                sinkedSSTs = storageManager.metas(SSTState.SINKED);
                lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, result.snapshotId());
                LOG.debug("Sink: newSST count={}, sinkedSST count={}", newSSTs.size(), sinkedSSTs.size());
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
            List<SSTMeta> newSsts = newSSTs;
            List<SSTMeta> sinkedSsts = sinkedSSTs;

            SequenceStats immutableStats = sequenceStats(immutables);
            SSTStats newStats = sstStats(newSsts);
            SSTStats sinkedStats = sstStats(sinkedSsts);

            return new BucketStateSnapshot(
                cur.estimatedEntryCount(),
                cur.estimatedSize(),
                immutables.size(),
                immutableStats.totalBytes(),
                walManager.lastSequenceId(),
                cur.minSequenceId(),
                cur.maxSequenceId(),
                immutableStats.minSequenceId(),
                immutableStats.maxSequenceId(),
                storageManager.lastFlushedSequenceId(),
                newSsts.size(),
                newStats.totalBytes(),
                newStats.minSequenceId(),
                newStats.maxSequenceId(),
                sinkedSsts.size(),
                sinkedStats.totalBytes(),
                0,
                0L,
                lastSinkedSnapshotId
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

    private Optional<Value> lookupSSTs(List<SSTMeta> ssts, Key key) {
        for (int i = ssts.size() - 1; i >= 0; i--) {
            Optional<Value> result = storageManager.get(ssts.get(i), key);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    private Optional<byte[]> bytesFromValue(Value value) {
        if (value.isTombstone()) {
            return Optional.empty();
        }
        return Optional.of(value.bytes());
    }

    private void refreshSSTLists() {
        newSSTs = storageManager.metas(SSTState.NEW);
        sinkedSSTs = storageManager.metas(SSTState.SINKED);
    }

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

    private static String nextBatchId(List<SSTMeta> ssts) {
        return "sink-" + maxSequenceId(ssts) + "-" + ssts.size();
    }

    private static long minSequenceId(List<SSTMeta> ssts) {
        return ssts.stream().mapToLong(SSTMeta::minSequenceId).min().orElse(0);
    }

    private static long maxSequenceId(List<SSTMeta> ssts) {
        return ssts.stream().mapToLong(SSTMeta::maxSequenceId).max().orElse(0);
    }

    private static SequenceStats sequenceStats(List<ImmutableMemTable> immutables) {
        long totalBytes = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;
        for (ImmutableMemTable im : immutables) {
            totalBytes += im.estimatedSize();
            if (im.minSequenceId() > 0 && (minSequenceId == 0 || im.minSequenceId() < minSequenceId)) {
                minSequenceId = im.minSequenceId();
            }
            maxSequenceId = Math.max(maxSequenceId, im.maxSequenceId());
        }
        return new SequenceStats(totalBytes, minSequenceId, maxSequenceId);
    }

    private static SSTStats sstStats(List<SSTMeta> ssts) {
        long totalBytes = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;
        for (SSTMeta sst : ssts) {
            totalBytes += sst.fileSize();
            if (sst.minSequenceId() > 0 && (minSequenceId == 0 || sst.minSequenceId() < minSequenceId)) {
                minSequenceId = sst.minSequenceId();
            }
            maxSequenceId = Math.max(maxSequenceId, sst.maxSequenceId());
        }
        return new SSTStats(totalBytes, minSequenceId, maxSequenceId);
    }

    private static class CollectingReplayCallback implements ReplayCallback {
        private final long lastFlushedSequenceId;
        final List<DataRecord> dataRecords = new ArrayList<>();
        final Set<Long> sinkedSSTIds = new HashSet<>();
        long skippedFlushedRecords;
        long lastSinkedSnapshotId;

        CollectingReplayCallback(long lastFlushedSequenceId) {
            this.lastFlushedSequenceId = lastFlushedSequenceId;
        }

        @Override
        public void onDataRecord(byte[] key, byte[] value) {
            dataRecords.add(new DataRecord(0, key, value));
        }

        @Override
        public void onDataRecord(long sequenceId, byte[] key, byte[] value) {
            if (sequenceId <= lastFlushedSequenceId) {
                skippedFlushedRecords++;
                return;
            }
            dataRecords.add(new DataRecord(sequenceId, key, value));
        }

        @Override
        public void onSinkPrepare(byte[] commitMessage) {
            if (commitMessage.length > 0) {
                // TODO: when the real Paimon sink is wired in, retain prepared commits
                // without matching SINK_SUCCESS and retry commit during recovery.
                SinkWalCodec.decodePrepare(commitMessage);
            }
        }

        @Override
        public void onSinkSuccess(long snapshotId) {
            lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, snapshotId);
        }

        @Override
        public void onSinkSuccess(long snapshotId, byte[] metadata) {
            onSinkSuccess(snapshotId);
            if (metadata.length == 0) {
                return;
            }
            SinkCommitResult result = SinkWalCodec.decodeSuccess(metadata);
            sinkedSSTIds.addAll(result.sstIds());
            lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, result.snapshotId());
        }
    }

    private record DataRecord(long sequenceId, byte[] key, byte[] value) {}

    private record RecoveryState(Set<Long> sinkedSSTIds, long lastSinkedSnapshotId) {}

    private record SequenceStats(long totalBytes, long minSequenceId, long maxSequenceId) {}

    private record SSTStats(long totalBytes, long minSequenceId, long maxSequenceId) {}
}
