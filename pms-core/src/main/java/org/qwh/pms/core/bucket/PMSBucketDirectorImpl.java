package org.qwh.pms.core.bucket;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.memtable.CurMemTable;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.MockSinkManager;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkCoordinator;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.sink.SinkRecoveryState;
import org.qwh.pms.core.sink.SinkWalReplayTracker;
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
import java.util.function.Function;

public class PMSBucketDirectorImpl implements PMSBucketDirector {

    private static final Logger LOG = LoggerFactory.getLogger(PMSBucketDirectorImpl.class);

    private final MemTableConfig memTableConfig;
    private final WALManagerImpl walManager;
    private final FileLocalStorageManager storageManager;
    private final SinkCoordinator sinkCoordinator;

    private volatile CurMemTable curMemTable;
    private volatile List<ImmutableMemTable> immutableMemTables = new ArrayList<>();
    private volatile List<SSTMeta> newSSTs = new ArrayList<>();
    private volatile List<SSTMeta> sinkedSSTs = new ArrayList<>();
    private volatile long lastSinkedSnapshotId;
    private volatile RecoverySummary lastRecoverySummary = RecoverySummary.empty();

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object writeMutex = new Object();
    private volatile boolean closed = false;

    public PMSBucketDirectorImpl(PMSConfig config) {
        this(config, new MockSinkManager());
    }

    public PMSBucketDirectorImpl(PMSConfig config, SinkManager sinkManager) {
        this(config, storageManager -> sinkManager);
    }

    public PMSBucketDirectorImpl(PMSConfig config, Function<FileLocalStorageManager, SinkManager> sinkManagerFactory) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(sinkManagerFactory, "sinkManagerFactory must not be null");
        this.memTableConfig = config.memtable();
        this.walManager = new WALManagerImpl(config);
        this.storageManager = new FileLocalStorageManager(config.storage());
        SinkManager sinkManager = Objects.requireNonNull(
            sinkManagerFactory.apply(storageManager),
            "sinkManagerFactory must not return null"
        );
        this.sinkCoordinator = new SinkCoordinator(sinkManager, walManager);
        this.curMemTable = new SkipListCurMemTable(memTableConfig);
    }

    public void init() throws IOException {
        storageManager.init();
        walManager.init();
        RecoveryState recoveryState = recoverFromWAL();
        SinkRecoveryState recoveredSink = recoverPreparedSinks(recoveryState.sinkRecoveryState());
        storageManager.applySinkedSSTIds(recoveredSink.sinkedSSTIds());
        refreshSSTLists();
        lastSinkedSnapshotId = recoveredSink.lastSinkedSnapshotId();
        lastRecoverySummary = new RecoverySummary(
            recoveryState.recoveredDataRecords(),
            recoveryState.skippedFlushedRecords(),
            recoveryState.lastFlushedSequenceId(),
            recoveryState.pendingPreparedSinkCount(),
            recoveredSink.sinkedSSTIds().size() - recoveryState.sinkedSSTCount(),
            recoveredSink.sinkedSSTIds().size(),
            lastSinkedSnapshotId,
            newSSTs.size(),
            sinkedSSTs.size(),
            curMemTable.estimatedEntryCount()
        );
        LOG.info("PMS recovery summary: {}", lastRecoverySummary);
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
        SinkRecoveryState sinkRecoveryState = cb.sinkReplay.state();
        return new RecoveryState(
            cb.dataRecords.size(),
            cb.skippedFlushedRecords,
            lastFlushedSequenceId,
            sinkRecoveryState.pendingPrepares().size(),
            sinkRecoveryState.sinkedSSTIds().size(),
            sinkRecoveryState
        );
    }

    private SinkRecoveryState recoverPreparedSinks(SinkRecoveryState state) {
        Set<Long> sinkedIds = new HashSet<>(state.sinkedSSTIds());
        long lastSnapshotId = state.lastSinkedSnapshotId();
        for (var prepared : state.pendingPrepares()) {
            SinkCommitResult result = sinkCoordinator.recoverPrepared(prepared);
            sinkedIds.addAll(result.sstIds());
            lastSnapshotId = Math.max(lastSnapshotId, result.snapshotId());
        }
        return new SinkRecoveryState(sinkedIds, List.of(), lastSnapshotId);
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
        SinkCommitResult result = sinkCoordinator.sink(batch);
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

    public RecoverySummary lastRecoverySummary() {
        return lastRecoverySummary;
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
        final SinkWalReplayTracker sinkReplay = new SinkWalReplayTracker();
        long skippedFlushedRecords;

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
            sinkReplay.onSinkPrepare(commitMessage);
        }

        @Override
        public void onSinkSuccess(long snapshotId) {
            sinkReplay.onSinkSuccess(snapshotId, new byte[0]);
        }

        @Override
        public void onSinkSuccess(long snapshotId, byte[] metadata) {
            sinkReplay.onSinkSuccess(snapshotId, metadata);
        }
    }

    private record DataRecord(long sequenceId, byte[] key, byte[] value) {}

    private record RecoveryState(
        long recoveredDataRecords,
        long skippedFlushedRecords,
        long lastFlushedSequenceId,
        long pendingPreparedSinkCount,
        long sinkedSSTCount,
        SinkRecoveryState sinkRecoveryState
    ) {}

    private record SequenceStats(long totalBytes, long minSequenceId, long maxSequenceId) {}

    private record SSTStats(long totalBytes, long minSequenceId, long maxSequenceId) {}
}
