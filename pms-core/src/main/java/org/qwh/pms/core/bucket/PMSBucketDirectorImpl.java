package org.qwh.pms.core.bucket;

import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.CurMemTable;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.MockSinkManager;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkCoordinator;
import org.qwh.pms.core.sink.SinkMetaStore;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.sink.SinkRecoveryState;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.core.storage.SSTMeta;
import org.qwh.pms.core.storage.SSTReadSnapshot;
import org.qwh.pms.core.storage.SSTState;
import org.qwh.pms.core.wal.ReplayCallback;
import org.qwh.pms.core.wal.WALManager.DataWrite;
import org.qwh.pms.core.wal.WALManagerImpl;
import org.qwh.pms.core.bucket.PMSBucketDirector.WriteOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class PMSBucketDirectorImpl implements PMSBucketDirector {

    private static final Logger LOG = LoggerFactory.getLogger(PMSBucketDirectorImpl.class);
    private static final int MAX_WRITE_BATCH_COUNT = 1024;
    private static final int MAX_WRITE_BATCH_BYTES = 4 * 1024 * 1024;

    private final MemTableConfig memTableConfig;
    private final StorageConfig storageConfig;
    private final WALManagerImpl walManager;
    private final FileLocalStorageManager storageManager;
    private final SinkMetaStore sinkMetaStore;
    private final SinkCoordinator sinkCoordinator;

    private volatile CurMemTable curMemTable;
    private volatile List<ImmutableMemTable> immutableMemTables = List.of();
    private volatile List<SSTMeta> newSSTs = List.of();
    private volatile List<SSTMeta> sinkedSSTs = List.of();
    private volatile long lastSinkedSnapshotId;
    private volatile RecoverySummary lastRecoverySummary = RecoverySummary.empty();

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object writeMutex = new Object();
    private final Object writeQueueMutex = new Object();
    private final ArrayDeque<WriteBatchRequest> pendingWrites = new ArrayDeque<>();
    private final Object sstMaintenanceMutex = new Object();
    private boolean writeLeaderActive;
    private volatile boolean closed = false;
    private volatile RuntimeException fatalFailure;

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
        this.storageConfig = config.storage();
        this.walManager = new WALManagerImpl(config);
        this.storageManager = new FileLocalStorageManager(config.storage());
        this.sinkMetaStore = new SinkMetaStore(Path.of(config.storage().dir()).resolve("sink"));
        SinkManager sinkManager = Objects.requireNonNull(
            sinkManagerFactory.apply(storageManager),
            "sinkManagerFactory must not return null"
        );
        this.sinkCoordinator = new SinkCoordinator(sinkManager, sinkMetaStore);
        this.curMemTable = new SkipListCurMemTable(memTableConfig);
    }

    public void init() throws IOException {
        storageManager.init();
        sinkMetaStore.init();
        walManager.init();
        RecoveryState recoveryState = recoverFromWAL();
        SinkRecoveryState initialSink = sinkMetaStore.load();
        SinkRecoveryState recoveredSink = recoverPreparedSinks(initialSink);
        storageManager.applySinkedSSTIds(recoveredSink.sinkedSSTIds(), recoveredSink.lastPersistedSequenceId());
        walManager.truncate(recoveredSink.lastPersistedSequenceId());
        refreshSSTLists();
        lastSinkedSnapshotId = recoveredSink.lastSinkedSnapshotId();
        lastRecoverySummary = new RecoverySummary(
            recoveryState.recoveredDataRecords(),
            recoveryState.skippedFlushedRecords(),
            recoveryState.lastFlushedSequenceId(),
            initialSink.pendingPrepares().size(),
            recoveredSink.sinkedSSTIds().size() - initialSink.sinkedSSTIds().size(),
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
        walManager.replay(cb);

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
        return new RecoveryState(
            cb.dataRecords.size(),
            cb.skippedFlushedRecords,
            lastFlushedSequenceId
        );
    }

    private SinkRecoveryState recoverPreparedSinks(SinkRecoveryState state) {
        Set<Long> sinkedIds = new HashSet<>(state.sinkedSSTIds());
        long lastSnapshotId = state.lastSinkedSnapshotId();
        long lastPersistedSequenceId = state.lastPersistedSequenceId();
        for (var prepared : state.pendingPrepares()) {
            SinkCommitResult result = sinkCoordinator.recoverPrepared(prepared);
            sinkedIds.addAll(result.sstIds());
            lastSnapshotId = Math.max(lastSnapshotId, result.snapshotId());
            lastPersistedSequenceId = Math.max(lastPersistedSequenceId, result.persistedSequenceId());
        }
        return new SinkRecoveryState(sinkedIds, List.of(), lastSnapshotId, lastPersistedSequenceId);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        writeBatch(List.of(WriteOp.put(key, value)));
    }

    @Override
    public void delete(byte[] key) {
        writeBatch(List.of(WriteOp.delete(key)));
    }

    @Override
    public void writeBatch(List<WriteOp> ops) {
        List<WriteOp> batch = validateWriteBatch(ops);
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            WriteBatchRequest request = new WriteBatchRequest(batch);
            boolean leader = enqueueWriteRequest(request);
            if (leader) {
                runWriteLeader();
            }
            request.await();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private List<WriteOp> validateWriteBatch(List<WriteOp> ops) {
        Objects.requireNonNull(ops, "ops must not be null");
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("ops must not be empty");
        }
        if (ops.size() > MAX_WRITE_BATCH_COUNT) {
            throw new IllegalArgumentException(
                "ops size exceeds max write batch count: " + ops.size() + " > " + MAX_WRITE_BATCH_COUNT
            );
        }
        for (WriteOp op : ops) {
            Objects.requireNonNull(op, "write op must not be null");
            if (op.key().length == 0) {
                throw new IllegalArgumentException("write op key must not be empty");
            }
        }
        return List.copyOf(ops);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return lookup(key).flatMap(this::bytesFromValue);
    }

    @Override
    public Optional<Value> lookup(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            Key k = new Key(key);

            Value v = curMemTable.get(k);
            if (v != null) {
                return Optional.of(v);
            }

            List<ImmutableMemTable> immutables = immutableMemTables;
            for (int i = immutables.size() - 1; i >= 0; i--) {
                v = immutables.get(i).get(k);
                if (v != null) {
                    return Optional.of(v);
                }
            }

            try (SSTReadSnapshot newSSTSnapshot = acquireNewSSTSnapshot()) {
                Optional<Value> newSSTValue = lookupSSTs(newSSTSnapshot, k);
                if (newSSTValue.isPresent()) {
                    return newSSTValue;
                }
            }
            try (SSTReadSnapshot sinkedSSTSnapshot = acquireSinkedSSTSnapshot()) {
                return lookupSSTs(sinkedSSTSnapshot, k);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> scan(byte[] startInclusive, Optional<byte[]> endExclusive) {
        Objects.requireNonNull(startInclusive, "startInclusive must not be null");
        Objects.requireNonNull(endExclusive, "endExclusive must not be null");
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            Key start = new Key(startInclusive);
            Optional<Key> end = endExclusive.map(Key::new);
            if (end.isPresent() && start.compareTo(end.get()) >= 0) {
                return List.of();
            }

            TreeMap<Key, Value> latest = new TreeMap<>();
            collectLatest(latest, curMemTable.iterator(start, end));

            List<ImmutableMemTable> immutables = immutableMemTables;
            for (ImmutableMemTable immutable : immutables) {
                collectLatest(latest, immutable.iterator(start, end));
            }

            try (SSTSnapshotPair snapshots = acquireSSTSnapshotPair()) {
                collectLatestFromSSTs(latest, snapshots.newSSTs(), start, end);
                collectLatestFromSSTs(latest, snapshots.sinkedSSTs(), start, end);
            }

            List<Entry> result = new ArrayList<>();
            for (Map.Entry<Key, Value> entry : latest.entrySet()) {
                if (!entry.getValue().isTombstone()) {
                    result.add(new Entry(entry.getKey(), entry.getValue()));
                }
            }
            return result;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> prefixScan(byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        return scan(prefix, prefixNext(prefix));
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
                immutableMemTables = List.copyOf(immutableList);

                List<SSTMeta> sstList = new ArrayList<>(newSSTs);
                sstList.add(meta);
                newSSTs = List.copyOf(sstList);
                LOG.debug("Flush: immutable count={}, newSST count={}", immutableList.size(), sstList.size());
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public Optional<SinkCommitResult> sinkToPaimon() {
        synchronized (sstMaintenanceMutex) {
            List<SSTMeta> toSink;
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                synchronized (writeMutex) {
                    if (newSSTs.isEmpty()) {
                        return Optional.empty();
                    }
                    toSink = List.copyOf(newSSTs);
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }

            SinkBatch batch = new SinkBatch(nextBatchId(toSink), toSink, minSequenceId(toSink), maxSequenceId(toSink));
            SinkCommitResult result = sinkCoordinator.sink(batch);
            storageManager.markSinked(toSink);
            walManager.truncate(result.persistedSequenceId());

            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                synchronized (writeMutex) {
                    Set<Long> sinkedIds = new HashSet<>(result.sstIds());
                    newSSTs = newSSTs.stream()
                        .filter(meta -> !sinkedIds.contains(meta.runId()))
                        .toList();
                    sinkedSSTs = storageManager.metas(SSTState.SINKED);
                    lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, result.snapshotId());
                    LOG.debug("Sink: newSST count={}, sinkedSST count={}", newSSTs.size(), sinkedSSTs.size());
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
            return Optional.of(result);
        }
    }

    @Override
    public Optional<SSTMeta> evictOldestSinkedSST() {
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                synchronized (writeMutex) {
                    if (sinkedSSTs.size() > storageConfig.sinkedMaxCount()) {
                        Optional<SSTMeta> compacted = compactOneGroupLocked(SSTState.SINKED);
                        if (compacted.isPresent() && sinkedSSTs.size() <= storageConfig.sinkedMaxCount()) {
                            LOG.debug(
                                "Skip sinked SST eviction after compaction: compactedRun={}, remainingSinkedSSTCount={}",
                                compacted.get().runId(),
                                sinkedSSTs.size()
                            );
                            return Optional.empty();
                        }
                    }
                    Optional<SSTMeta> evicted = storageManager.evictOldestSinkedSST();
                    if (evicted.isPresent()) {
                        long evictedRunId = evicted.get().runId();
                        sinkedSSTs = sinkedSSTs.stream()
                            .filter(meta -> meta.runId() != evictedRunId)
                            .toList();
                        LOG.debug(
                            "Evicted sinked SST: runId={}, remainingSinkedSSTCount={}",
                            evictedRunId,
                            sinkedSSTs.size()
                        );
                    }
                    return evicted;
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Override
    public void compactLocalSSTs() {
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                synchronized (writeMutex) {
                    compactOneGroupLocked(SSTState.NEW);
                    compactOneGroupLocked(SSTState.SINKED);
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
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
                newStats.totalRows(),
                newStats.minSequenceId(),
                newStats.maxSequenceId(),
                sinkedSsts.size(),
                sinkedStats.totalBytes(),
                sinkedStats.totalRows(),
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
            storageManager.close();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private Optional<Value> lookupSSTs(SSTReadSnapshot snapshot, Key key) {
        List<SSTMeta> ssts = snapshot.metas();
        for (int i = ssts.size() - 1; i >= 0; i--) {
            Optional<Value> result = snapshot.get(ssts.get(i), key);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    private boolean enqueueWriteRequest(WriteBatchRequest request) {
        synchronized (writeQueueMutex) {
            pendingWrites.addLast(request);
            if (!writeLeaderActive) {
                writeLeaderActive = true;
                return true;
            }
            return false;
        }
    }

    private void runWriteLeader() {
        while (true) {
            List<WriteBatchRequest> batch = drainWriteBatch();
            if (batch.isEmpty()) {
                synchronized (writeQueueMutex) {
                    if (pendingWrites.isEmpty()) {
                        writeLeaderActive = false;
                        return;
                    }
                    continue;
                }
            }
            processWriteBatch(batch);
        }
    }

    private List<WriteBatchRequest> drainWriteBatch() {
        synchronized (writeQueueMutex) {
            if (pendingWrites.isEmpty()) {
                return List.of();
            }
            List<WriteBatchRequest> batch = new ArrayList<>();
            int batchOpCount = 0;
            long batchBytes = 0;
            while (!pendingWrites.isEmpty()) {
                WriteBatchRequest next = pendingWrites.peekFirst();
                int nextOpCount = next.opCount();
                long nextBytes = next.estimatedWalBytes();
                boolean wouldExceedCount = batchOpCount + nextOpCount > MAX_WRITE_BATCH_COUNT;
                boolean wouldExceedBytes = batchBytes + nextBytes > MAX_WRITE_BATCH_BYTES;
                if (!batch.isEmpty() && (wouldExceedCount || wouldExceedBytes)) {
                    break;
                }
                batch.add(pendingWrites.removeFirst());
                batchOpCount += nextOpCount;
                batchBytes += nextBytes;
            }
            return batch;
        }
    }

    private void processWriteBatch(List<WriteBatchRequest> batch) {
        Throwable failure = null;
        boolean walAppended = false;
        try {
            synchronized (writeMutex) {
                List<DataWrite> writes = flattenWrites(batch);
                long sequenceBegin = walManager.appendDataRecords(writes);
                walAppended = true;
                int sequenceOffset = 0;
                for (WriteBatchRequest request : batch) {
                    for (WriteOp op : request.ops()) {
                        long sequenceId = sequenceBegin + sequenceOffset++;
                        Value value = op.value() != null
                            ? new Value(op.value(), sequenceId)
                            : Value.tombstone(sequenceId);
                        curMemTable.put(new Key(op.key()), value);
                    }
                }
                maybeFreezeLocked();
            }
        } catch (Throwable t) {
            failure = walAppended ? markFatalAfterWalAppend(t) : t;
        }
        for (WriteBatchRequest request : batch) {
            request.complete(failure);
        }
    }

    private List<DataWrite> flattenWrites(List<WriteBatchRequest> batch) {
        List<DataWrite> writes = new ArrayList<>(totalOpCount(batch));
        for (WriteBatchRequest request : batch) {
            for (WriteOp op : request.ops()) {
                writes.add(new DataWrite(op.key(), op.value()));
            }
        }
        return writes;
    }

    private static int totalOpCount(List<WriteBatchRequest> batch) {
        int count = 0;
        for (WriteBatchRequest request : batch) {
            count += request.opCount();
        }
        return count;
    }

    private RuntimeException markFatalAfterWalAppend(Throwable failure) {
        RuntimeException fatal = new IllegalStateException(
            "WAL append succeeded but MemTable apply failed; PMSBucketDirector must be restarted",
            failure
        );
        fatalFailure = fatal;
        return fatal;
    }

    private void collectLatestFromSSTs(TreeMap<Key, Value> latest, SSTReadSnapshot snapshot, Key start, Optional<Key> end) {
        for (SSTMeta sst : snapshot.metas()) {
            try (var iterator = snapshot.openIterator(sst, start, end)) {
                collectLatest(latest, iterator);
            }
        }
    }

    private SSTReadSnapshot acquireNewSSTSnapshot() {
        synchronized (writeMutex) {
            return storageManager.readSnapshot(newSSTs);
        }
    }

    private SSTReadSnapshot acquireSinkedSSTSnapshot() {
        synchronized (writeMutex) {
            return storageManager.readSnapshot(sinkedSSTs);
        }
    }

    private SSTSnapshotPair acquireSSTSnapshotPair() {
        synchronized (writeMutex) {
            SSTReadSnapshot newSnapshot = storageManager.readSnapshot(newSSTs);
            try {
                SSTReadSnapshot sinkedSnapshot = storageManager.readSnapshot(sinkedSSTs);
                return new SSTSnapshotPair(newSnapshot, sinkedSnapshot);
            } catch (RuntimeException e) {
                try {
                    newSnapshot.close();
                } catch (RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }
    }

    private static void collectLatest(TreeMap<Key, Value> latest, Iterator<Entry> iterator) {
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            Value current = latest.get(entry.key());
            if (current == null || entry.value().sequenceId() > current.sequenceId()) {
                latest.put(entry.key(), entry.value());
            }
        }
    }

    private static Optional<byte[]> prefixNext(byte[] prefix) {
        byte[] next = Arrays.copyOf(prefix, prefix.length);
        for (int i = next.length - 1; i >= 0; i--) {
            int value = next[i] & 0xFF;
            if (value != 0xFF) {
                next[i] = (byte) (value + 1);
                return Optional.of(Arrays.copyOf(next, i + 1));
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

    private Optional<SSTMeta> compactOneGroupLocked(SSTState state) {
        List<SSTMeta> source = state == SSTState.NEW ? newSSTs : sinkedSSTs;
        Optional<List<SSTMeta>> group = selectCompactionGroup(source);
        if (group.isEmpty()) {
            return Optional.empty();
        }
        SSTMeta compacted = storageManager.compactSSTs(group.get());
        refreshSSTLists();
        LOG.debug(
            "Compacted local SSTs: state={}, inputCount={}, outputRunId={}, flushRange=[{},{}]",
            state,
            group.get().size(),
            compacted.runId(),
            compacted.minFlushId(),
            compacted.maxFlushId()
        );
        return Optional.of(compacted);
    }

    private Optional<List<SSTMeta>> selectCompactionGroup(List<SSTMeta> ssts) {
        if (ssts.size() < storageConfig.compactMinFiles()) {
            return Optional.empty();
        }
        long maxGroupBytes = storageConfig.compactThresholdMb() * 1024L * 1024L;
        List<SSTMeta> group = new ArrayList<>();
        long groupBytes = 0;
        long previousMaxFlushId = -1;
        for (SSTMeta sst : ssts) {
            boolean continuous = group.isEmpty() || previousMaxFlushId + 1 == sst.minFlushId();
            boolean fits = sst.fileSize() <= maxGroupBytes && groupBytes + sst.fileSize() <= maxGroupBytes;
            if (!continuous || !fits) {
                if (group.size() >= storageConfig.compactMinFiles()) {
                    return Optional.of(List.copyOf(group));
                }
                group.clear();
                groupBytes = 0;
                previousMaxFlushId = -1;
            }
            if (sst.fileSize() <= maxGroupBytes) {
                group.add(sst);
                groupBytes += sst.fileSize();
                previousMaxFlushId = sst.maxFlushId();
            }
        }
        return group.size() >= storageConfig.compactMinFiles()
            ? Optional.of(List.copyOf(group))
            : Optional.empty();
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
        immutableMemTables = List.copyOf(newList);
        LOG.debug("Freeze: immutable count={}", newList.size());
    }

    private void ensureNotClosed() {
        if (fatalFailure != null) {
            throw fatalFailure;
        }
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
        long totalRows = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;
        for (SSTMeta sst : ssts) {
            totalBytes += sst.fileSize();
            totalRows += sst.entryCount();
            if (sst.minSequenceId() > 0 && (minSequenceId == 0 || sst.minSequenceId() < minSequenceId)) {
                minSequenceId = sst.minSequenceId();
            }
            maxSequenceId = Math.max(maxSequenceId, sst.maxSequenceId());
        }
        return new SSTStats(totalBytes, totalRows, minSequenceId, maxSequenceId);
    }

    private static final class WriteBatchRequest {
        private final List<WriteOp> ops;
        private final CountDownLatch done = new CountDownLatch(1);
        private Throwable failure;

        private WriteBatchRequest(List<WriteOp> ops) {
            this.ops = ops;
        }

        private List<WriteOp> ops() {
            return ops;
        }

        private int opCount() {
            return ops.size();
        }

        private long estimatedWalBytes() {
            long bytes = 1L + Long.BYTES + Integer.BYTES;
            for (WriteOp op : ops) {
                bytes += Integer.BYTES + op.key().length + Integer.BYTES;
                if (op.value() != null) {
                    bytes += op.value().length;
                }
            }
            return bytes;
        }

        private void complete(Throwable failure) {
            this.failure = failure;
            done.countDown();
        }

        private void await() {
            boolean interrupted = false;
            while (true) {
                try {
                    done.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (failure != null) {
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new RuntimeException("write failed", failure);
            }
        }
    }

    private static class CollectingReplayCallback implements ReplayCallback {
        private final long lastFlushedSequenceId;
        final List<DataRecord> dataRecords = new ArrayList<>();
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

    }

    private record DataRecord(long sequenceId, byte[] key, byte[] value) {}

    private record RecoveryState(
        long recoveredDataRecords,
        long skippedFlushedRecords,
        long lastFlushedSequenceId
    ) {}

    private record SequenceStats(long totalBytes, long minSequenceId, long maxSequenceId) {}

    private record SSTStats(long totalBytes, long totalRows, long minSequenceId, long maxSequenceId) {}

    private record SSTSnapshotPair(SSTReadSnapshot newSSTs, SSTReadSnapshot sinkedSSTs) implements AutoCloseable {

        @Override
        public void close() {
            RuntimeException failure = null;
            try {
                newSSTs.close();
            } catch (RuntimeException e) {
                failure = e;
            }
            try {
                sinkedSSTs.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
