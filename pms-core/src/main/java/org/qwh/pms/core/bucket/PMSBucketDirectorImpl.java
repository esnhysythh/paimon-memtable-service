package org.qwh.pms.core.bucket;

import org.qwh.pms.core.config.FlowControlConfig;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.bucket.operation.CompactionResult;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.EvictionResult;
import org.qwh.pms.core.bucket.operation.FlushResult;
import org.qwh.pms.core.bucket.operation.FreezeResult;
import org.qwh.pms.core.bucket.operation.OperationStatus;
import org.qwh.pms.core.bucket.operation.SinkOperationResult;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.memtable.CurMemTable;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkCoordinator;
import org.qwh.pms.core.sink.SinkMetaStore;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.sink.PreparedSinkCommit;
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
import java.util.Comparator;
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
    private static final int MAX_WRITE_BATCH_BYTES = 4 * 1024 * 1024;

    private final MemTableConfig memTableConfig;
    private final FlowControlConfig flowControlConfig;
    private final WALManagerImpl walManager;
    private final FileLocalStorageManager storageManager;
    private final SinkMetaStore sinkMetaStore;
    private final SinkCoordinator sinkCoordinator;
    private volatile MemTableState memTables;
    private volatile RunState runState = RunState.empty();
    private volatile SinkFlightSnapshot sinkFlight = SinkFlightSnapshot.idle();
    /** Protected by flushMutex; retained only while this process retries one Flush handoff. */
    private FlushFlight flushFlight;
    private volatile long recoveredUnpersistedMaxSequenceId;
    private volatile long lastFlushedSequenceId;
    private volatile long lastPersistedSequenceId;
    private volatile long lastSinkedSnapshotId;
    private volatile RecoverySummary lastRecoverySummary = RecoverySummary.empty();

    /*
     * Nested lock order is flushMutex/sstMaintenanceMutex -> lifecycleLock -> writeMutex or
     * runStateMutex. writeMutex and runStateMutex are separate publication domains and are never
     * nested with each other. writeQueueMutex is never held while performing WAL, storage, or
     * sink work.
     */
    /**
     * Operation lease: public operations hold the read side for their full duration, including
     * slow I/O; close() takes the write side before closing WAL and local storage.
     * This lock does not protect bucket data consistency.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    /**
     * Serializes the WAL append -> MemTable apply -> freeze boundary and short MemTableState
     * publication updates. Queries, state snapshots, and SST maintenance never acquire this lock.
     */
    private final Object writeMutex = new Object();
    /** Serializes Flush selection and publication so one immutable cannot be flushed twice. */
    private final Object flushMutex = new Object();
    private final Object writeQueueMutex = new Object();
    private final ArrayDeque<WriteBatchRequest> pendingWrites = new ArrayDeque<>();
    /**
     * Serializes short immutable RunState delta publications from Flush and SST maintenance.
     * Storage I/O and Paimon I/O must never run while holding this mutex.
     */
    private final Object runStateMutex = new Object();
    /**
     * V1 permits one SST maintenance operation at a time. A Sink may leave a recoverable flight
     * after durable prepare or durable success; its logical flush fence then excludes the selected
     * NEW prefix from later compaction until recovery completes.
     */
    private final Object sstMaintenanceMutex = new Object();
    private boolean writeLeaderActive;
    private volatile boolean closed = false;
    private volatile RuntimeException fatalFailure;

    public PMSBucketDirectorImpl(PMSConfig config, Function<FileLocalStorageManager, SinkManager> sinkManagerFactory) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(sinkManagerFactory, "sinkManagerFactory must not be null");
        this.memTableConfig = config.memtable();
        this.flowControlConfig = config.flowcontrol();
        this.walManager = new WALManagerImpl(config);
        this.storageManager = new FileLocalStorageManager(config.storage());
        this.sinkMetaStore = new SinkMetaStore(Path.of(config.storage().dir()).resolve("sink"));
        SinkManager sinkManager = Objects.requireNonNull(
            sinkManagerFactory.apply(storageManager),
            "sinkManagerFactory must not return null"
        );
        this.sinkCoordinator = new SinkCoordinator(sinkManager, sinkMetaStore);
        this.memTables = new MemTableState(new SkipListCurMemTable(memTableConfig), List.of());
    }

    public void init() throws IOException {
        storageManager.init();
        sinkMetaStore.init();
        walManager.init();
        RecoveryState recoveryState = recoverFromWAL();
        SinkRecoveryState initialSink = sinkMetaStore.load();
        validateSinglePendingPrepare(initialSink);
        sinkFlight = sinkFlightFromRecovery(initialSink);
        SinkRecoveryState recoveredSink = recoverPreparedSinks(initialSink);
        storageManager.applySinkedSSTIds(recoveredSink.sinkedSSTIds(), recoveredSink.lastPersistedSequenceId());
        walManager.truncate(recoveredSink.lastPersistedSequenceId());
        sinkFlight = SinkFlightSnapshot.idle();
        runState = RunState.fromMetas(storageManager.metas());
        RunState recoveredRuns = runState;
        lastFlushedSequenceId = recoveryState.lastFlushedSequenceId();
        lastPersistedSequenceId = recoveredSink.lastPersistedSequenceId();
        recoveredUnpersistedMaxSequenceId = Math.max(
            recoveryState.maxRecoveredSequenceId(),
            maxSequenceId(metas(recoveredRuns.newRuns()))
        );
        lastSinkedSnapshotId = recoveredSink.lastSinkedSnapshotId();
        lastRecoverySummary = new RecoverySummary(
            recoveryState.recoveredDataRecords(),
            recoveryState.skippedFlushedRecords(),
            recoveryState.lastFlushedSequenceId(),
            initialSink.pendingPrepares().size(),
            recoveredSink.sinkedSSTIds().size() - initialSink.sinkedSSTIds().size(),
            recoveredSink.sinkedSSTIds().size(),
            lastSinkedSnapshotId,
            recoveredRuns.newRuns().size(),
            recoveredRuns.sinkedRuns().size(),
            memTables.current().estimatedEntryCount()
        );
        LOG.info("PMS recovery summary: {}", lastRecoverySummary);
        LOG.info("PMSBucketDirector initialized, curMemTable entries={}", memTables.current().estimatedEntryCount());
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
            memTables.current().put(key, value);
        }
        long maxRecoveredSequenceId = cb.dataRecords.stream()
            .mapToLong(record -> record.sequenceId)
            .max()
            .orElse(0);
        LOG.info(
            "Recovered {} data records from WAL, skippedFlushedRecords={}, lastFlushedSequenceId={}, maxRecoveredSequenceId={}",
            cb.dataRecords.size(),
            cb.skippedFlushedRecords,
            lastFlushedSequenceId,
            maxRecoveredSequenceId
        );
        return new RecoveryState(
            cb.dataRecords.size(),
            cb.skippedFlushedRecords,
            lastFlushedSequenceId,
            maxRecoveredSequenceId
        );
    }

    private SinkRecoveryState recoverPreparedSinks(SinkRecoveryState state) {
        Set<Long> sinkedIds = new HashSet<>(state.sinkedSSTIds());
        long lastSnapshotId = state.lastSinkedSnapshotId();
        long lastPersistedSequenceId = state.lastPersistedSequenceId();
        for (var prepared : state.pendingPrepares()) {
            SinkCommitResult result = sinkCoordinator.commitPrepared(prepared);
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
            MemTableState memView = memTables;
            Value v = memView.current().get(k);
            if (v != null) {
                return Optional.of(v);
            }

            List<ImmutableMemTable> immutables = memView.immutables();
            for (int i = immutables.size() - 1; i >= 0; i--) {
                v = immutables.get(i).get(k);
                if (v != null) {
                    return Optional.of(v);
                }
            }

            // Storage captures its visible meta view and enters the read epoch atomically.
            // A Flush publishes the SST before removing its immutable source, so the query
            // always observes at least one side of that handoff.
            try (SSTReadSnapshot snapshot = storageManager.readVisibleSnapshot()) {
                return lookupSSTs(snapshot, k);
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

            // The immutable MemTableState keeps the old active object alive across Freeze.
            // Iteration remains weakly consistent with later writes; V1 does not promise an
            // MVCC or batch-atomic scan.
            MemTableState memView = memTables;
            Iterator<Entry> curIterator = memView.current().iterator(start, end);
            List<Iterator<Entry>> immutableIterators = memView.immutables().stream()
                .map(immutable -> immutable.iterator(start, end))
                .toList();

            TreeMap<Key, Value> latest = new TreeMap<>();
            collectLatest(latest, curIterator);
            for (Iterator<Entry> immutableIterator : immutableIterators) {
                collectLatest(latest, immutableIterator);
            }

            try (SSTReadSnapshot snapshot = storageManager.readVisibleSnapshot()) {
                collectLatestFromSSTs(latest, snapshot, start, end);
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
    public FreezeResult freezeCurMemTable() {
        lifecycleLock.readLock().lock();
        try {
            ensureNotClosed();
            synchronized (writeMutex) {
                return doFreezeLocked();
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public FlushResult flushImmutableMemTable() {
        synchronized (flushMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                FlushFlight flight = flushFlight;
                if (flight == null) {
                    ImmutableMemTable toFlush;
                    synchronized (writeMutex) {
                        if (memTables.immutables().isEmpty()) {
                            return FlushResult.noop();
                        }
                        toFlush = memTables.immutables().get(0);
                    }

                    // Keep the lifecycle lease across I/O. close() must not close storage while an
                    // operation selected under the lease is still using it.
                    // Once Storage publishes an SST, every in-process retry must finish this exact
                    // output. Re-flushing the same immutable would create a second visible run.
                    SSTMeta output = storageManager.flushToSST(toFlush);
                    flight = new FlushFlight(toFlush, output);
                    flushFlight = flight;
                }

                ImmutableMemTable toFlush = flight.source();
                SSTMeta meta = flight.output();
                storageManager.persistFlushedSequenceId(meta.maxSequenceId());

                synchronized (writeMutex) {
                    MemTableState currentState = memTables;
                    List<ImmutableMemTable> immutableList = new ArrayList<>(currentState.immutables());
                    if (!immutableList.remove(toFlush)) {
                        throw new IllegalStateException("selected immutable MemTable is no longer visible");
                    }
                    memTables = new MemTableState(currentState.current(), immutableList);
                    lastFlushedSequenceId = Math.max(lastFlushedSequenceId, meta.maxSequenceId());
                }
                // A run becomes maintenance-eligible only after both its durable flush boundary
                // and the MemTable -> SST query handoff have completed.
                LocalRun output = publishFlushedRun(meta);
                // Clear before logging/result construction: all stateful Flush steps are complete.
                flushFlight = null;
                LOG.debug(
                    "Flush: immutable count={}, newSST count={}",
                    memTables.immutables().size(),
                    runState.newRuns().size()
                );
                return new FlushResult(
                    OperationStatus.PROGRESSED,
                    Optional.of(output.snapshot())
                );
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Override
    public SinkOperationResult sinkToPaimon(SinkSelection selection) {
        Objects.requireNonNull(selection, "selection must not be null");
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                if (sinkFlight.active()) {
                    throw new IllegalStateException(
                        "cannot start a new Sink while another Sink is " + sinkFlight.status()
                    );
                }
                List<LocalRun> toSink = selectSinkPrefix(selection, runState.newRuns());
                if (toSink.isEmpty()) {
                    return SinkOperationResult.noop();
                }
                List<SSTMeta> toSinkMetas = metas(toSink);
                SinkFlightSnapshot flight = new SinkFlightSnapshot(
                    SinkFlightSnapshot.Status.IN_FLIGHT,
                    nextBatchId(toSinkMetas),
                    maxFlushId(toSinkMetas),
                    minSequenceId(toSinkMetas),
                    maxSequenceId(toSinkMetas)
                );
                sinkFlight = flight;
                SinkBatch batch = new SinkBatch(
                    flight.batchId(),
                    toSinkMetas,
                    minSequenceId(toSinkMetas),
                    maxSequenceId(toSinkMetas)
                );
                SinkCommitResult result;
                try {
                    result = sinkCoordinator.sink(batch);
                    // Durable success now exists. Publish this state before the first local mutation
                    // so every later failure is recovered as finalization, never as a new commit.
                    sinkFlight = sinkFlightForCommitted(result, toSink);
                    return finalizeCommittedSink(result, toSink);
                } catch (RuntimeException e) {
                    refreshSinkFlightAfterFailureUnderLease();
                    throw e;
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Override
    public SinkOperationResult resumeSinkFlight() {
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                if (!sinkFlight.active()) {
                    return SinkOperationResult.noop();
                }
                try {
                    return switch (sinkFlight.status()) {
                        case PREPARED_RETRY -> resumePreparedSink();
                        case FINALIZING -> resumeCommittedSinkFinalization();
                        case IN_FLIGHT -> throw new IllegalStateException(
                            "cannot resume a Sink while its original call is still in flight: batch="
                                + sinkFlight.batchId()
                        );
                        case IDLE -> SinkOperationResult.noop();
                    };
                } catch (RuntimeException e) {
                    refreshSinkFlightAfterFailureUnderLease();
                    throw e;
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Override
    public EvictionResult evictOldestSinkedSST() {
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                List<LocalRun> sinkedRuns = runState.sinkedRuns();
                if (sinkedRuns.isEmpty()) {
                    return EvictionResult.noop();
                }
                LocalRun oldest = sinkedRuns.get(0);
                storageManager.deleteSST(oldest.meta());
                LocalRun evictedRun = publishEvictedRun(oldest.runId());
                LOG.debug(
                    "Evicted sinked SST: runId={}, remainingSinkedSSTCount={}",
                    evictedRun.runId(),
                    runState.sinkedRuns().size()
                );
                return new EvictionResult(
                    OperationStatus.PROGRESSED,
                    Optional.of(evictedRun.snapshot())
                );
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Override
    public CompactionResult compactLocalSSTs(CompactionSelection selection) {
        Objects.requireNonNull(selection, "selection must not be null");
        synchronized (sstMaintenanceMutex) {
            lifecycleLock.readLock().lock();
            try {
                ensureNotClosed();
                Optional<List<LocalRun>> selected = resolveCompactionSelection(selection);
                if (selected.isEmpty()) {
                    return CompactionResult.noop();
                }
                CompactionResult.Group group = compactSelectedGroup(selection.state(), selected.get());
                return new CompactionResult(OperationStatus.PROGRESSED, Optional.of(group));
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
            long nowMillis = nowMillis();
            MemTableState memView = memTables;
            CurMemTable cur = memView.current();
            List<ImmutableMemTable> immutables = memView.immutables();
            List<LocalRun> visibleRuns = localRuns(storageManager.metas());
            List<LocalRun> newRunSnapshot = visibleRuns.stream()
                .filter(run -> run.meta().state() == SSTState.NEW)
                .toList();
            List<LocalRun> sinkedRunSnapshot = visibleRuns.stream()
                .filter(run -> run.meta().state() == SSTState.SINKED)
                .toList();
            SinkFlightSnapshot sinkFlightView = sinkFlight;

            // MemTable metrics may include or exclude a truly concurrent write. Structural
            // Freeze/Flush transitions are observed through immutable MemTableState and the
            // storage-owned meta view, so snapshot construction never blocks write/query paths.
            SequenceStats immutableStats = sequenceStats(immutables);
            RunStats newStats = runStats(newRunSnapshot);
            RunStats sinkedStats = runStats(sinkedRunSnapshot);
            List<LocalRunSnapshot> localRuns = visibleRuns.stream()
                .map(LocalRun::snapshot)
                .toList();

            return new BucketStateSnapshot(
                nowMillis,
                cur.estimatedEntryCount(),
                cur.estimatedSize(),
                cur.minSequenceId(),
                cur.maxSequenceId(),
                cur.oldestWriteAtMillis(),
                immutables.size(),
                immutableStats.totalBytes(),
                immutableStats.minSequenceId(),
                immutableStats.maxSequenceId(),
                immutableStats.oldestWriteAtMillis(),
                walManager.lastSequenceId(),
                lastFlushedSequenceId,
                lastPersistedSequenceId,
                newRunSnapshot.size(),
                newStats.totalBytes(),
                newStats.totalRows(),
                newStats.minSequenceId(),
                newStats.maxSequenceId(),
                newStats.oldestWriteAtMillis(),
                sinkedRunSnapshot.size(),
                sinkedStats.totalBytes(),
                sinkedStats.totalRows(),
                sinkedStats.minSequenceId(),
                sinkedStats.maxSequenceId(),
                sinkedStats.oldestWriteAtMillis(),
                localRuns,
                sinkFlightView,
                recoveredUnpersistedMaxSequenceId > lastPersistedSequenceId,
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
                rejectWriteIfOverloadedLocked();
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
                        memTables.current().put(new Key(op.key()), value);
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

    /**
     * Rechecks the admission watermarks at the actual serialized write boundary. This does
     * not reserve capacity or prevent a concurrently published Flush result from moving a count;
     * it only guarantees that a batch observing an already-overloaded state never enters WAL.
     */
    private void rejectWriteIfOverloadedLocked() {
        int immutableCount = memTables.immutables().size();
        int newSstCount = runState.newRuns().size();
        if (immutableCount >= flowControlConfig.overloadedImmutableCount()
                || newSstCount >= flowControlConfig.overloadedPendingSstCount()) {
            throw new PmsWriteOverloadedException(
                "PMS write backlog reached the overload watermark: immutableCount="
                    + immutableCount
                    + "/"
                    + flowControlConfig.overloadedImmutableCount()
                    + ", newSstCount="
                    + newSstCount
                    + "/"
                    + flowControlConfig.overloadedPendingSstCount()
            );
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
        RuntimeException fatal = new PmsFatalWriteException(
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

    /** Caller must hold a lifecycle read lease. */
    private void refreshSinkFlightAfterFailureUnderLease() {
        SinkFlightSnapshot failedFlight = sinkFlight;
        if (!failedFlight.active()) {
            return;
        }
        // A success file wins over the still-present prepare file. Retrying prepare after success
        // could create fresh Paimon files whose payload does not describe the committed snapshot.
        Optional<SinkCommitResult> durableSuccess = sinkMetaStore.loadSuccess(failedFlight.batchId());
        if (durableSuccess.isPresent()) {
            List<LocalRun> selected = resolveCommittedRuns(durableSuccess.get(), runState);
            sinkFlight = sinkFlightForCommitted(durableSuccess.get(), selected);
            return;
        }

        SinkRecoveryState recovery = sinkMetaStore.load();
        validateSinglePendingPrepare(recovery);
        if (!recovery.pendingPrepares().isEmpty()) {
            PreparedSinkCommit prepared = recovery.pendingPrepares().get(0);
            if (!prepared.batchId().equals(failedFlight.batchId())) {
                throw new IllegalStateException(
                    "active Sink flight differs from durable prepare metadata: active="
                        + failedFlight.batchId()
                        + ", durable="
                        + prepared.batchId()
                );
            }
            List<LocalRun> selected = resolvePreparedRuns(prepared, runState.newRuns());
            sinkFlight = sinkFlightForPrepared(prepared, selected);
            return;
        }
        if (failedFlight.status() != SinkFlightSnapshot.Status.IN_FLIGHT) {
            throw new IllegalStateException(
                "recoverable Sink flight has no durable metadata: batch=" + failedFlight.batchId()
                    + ", status=" + failedFlight.status()
            );
        }
        sinkFlight = SinkFlightSnapshot.idle();
    }

    private static List<LocalRun> localRuns(List<SSTMeta> metas) {
        return metas.stream()
            .map(LocalRun::new)
            .toList();
    }

    private static List<SSTMeta> metas(List<LocalRun> runs) {
        return runs.stream().map(LocalRun::meta).toList();
    }

    private static long nowMillis() {
        return System.currentTimeMillis();
    }

    private SinkOperationResult resumePreparedSink() {
        SinkRecoveryState recovery = sinkMetaStore.load();
        validateSinglePendingPrepare(recovery);
        if (recovery.pendingPrepares().isEmpty()) {
            throw new IllegalStateException(
                "PREPARED_RETRY Sink flight has no pending prepare metadata: batch=" + sinkFlight.batchId()
            );
        }
        PreparedSinkCommit prepared = recovery.pendingPrepares().get(0);
        List<LocalRun> selected = resolvePreparedRuns(prepared, runState.newRuns());
        SinkFlightSnapshot expectedFlight = sinkFlightForPrepared(prepared, selected);
        if (!sinkFlight.equals(expectedFlight)) {
            throw new IllegalStateException(
                "prepared Sink flight differs from durable prepare metadata: batch=" + prepared.batchId()
            );
        }
        SinkCommitResult result = sinkCoordinator.commitPrepared(prepared);
        sinkFlight = sinkFlightForCommitted(result, selected);
        return finalizeCommittedSink(result, selected);
    }

    private SinkOperationResult resumeCommittedSinkFinalization() {
        // Deliberately bypass SinkCoordinator: FINALIZING must never invoke Paimon prepare/commit.
        SinkCommitResult result = sinkMetaStore.loadSuccess(sinkFlight.batchId())
            .orElseThrow(() -> new IllegalStateException(
                "FINALIZING Sink flight has no durable success metadata: batch=" + sinkFlight.batchId()
            ));
        List<LocalRun> selected = resolveCommittedRuns(result, runState);
        SinkFlightSnapshot expectedFlight = sinkFlightForCommitted(result, selected);
        if (!sinkFlight.equals(expectedFlight)) {
            throw new IllegalStateException(
                "finalizing Sink flight differs from durable success metadata: batch=" + result.batchId()
            );
        }
        return finalizeCommittedSink(result, selected);
    }

    private SinkOperationResult finalizeCommittedSink(
            SinkCommitResult result,
            List<LocalRun> selectedRuns) {
        List<Long> selectedRunIds = selectedRuns.stream().map(LocalRun::runId).toList();
        if (!result.sstIds().equals(selectedRunIds)) {
            throw new IllegalStateException(
                "committed Sink result differs from maintenance-visible inputs: batch=" + result.batchId()
            );
        }
        Set<Long> sinkedIds = Set.copyOf(selectedRunIds);
        // Both steps are ensure-style operations. Repeating them after a partial previous attempt
        // converges to the same metadata and immutable RunState without duplicating runs.
        List<SSTMeta> selectedSinkedMetas = storageManager.markSinked(metas(selectedRuns));
        List<LocalRun> publishedSinkedRuns = ensureSinkedRuns(sinkedIds, selectedSinkedMetas);
        lastPersistedSequenceId = Math.max(lastPersistedSequenceId, result.persistedSequenceId());
        lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, result.snapshotId());
        List<LocalRunSnapshot> sinked = publishedSinkedRuns.stream()
            .map(LocalRun::snapshot)
            .toList();
        SinkOperationResult operationResult = new SinkOperationResult(
            OperationStatus.PROGRESSED,
            Optional.of(result),
            sinked
        );
        // At this point every logical local state derived from durable success is published.
        // WAL deletion is only space reclamation, so it must not keep the Sink flight active.
        sinkFlight = SinkFlightSnapshot.idle();
        try {
            walManager.truncate(result.persistedSequenceId());
        } catch (RuntimeException e) {
            LOG.warn(
                "Failed to truncate WAL after durable Sink success: batch={}, persistedSequenceId={}",
                result.batchId(),
                result.persistedSequenceId(),
                e
            );
        }
        RunState published = runState;
        LOG.debug(
            "Sink completed: batch={}, newSST count={}, sinkedSST count={}",
            result.batchId(),
            published.newRuns().size(),
            published.sinkedRuns().size()
        );
        return operationResult;
    }

    private CompactionResult.Group compactSelectedGroup(SSTState state, List<LocalRun> selected) {
        List<Long> inputRunIds = selected.stream().map(LocalRun::runId).toList();
        SSTMeta compacted = storageManager.compactSSTs(metas(selected));
        LocalRun output = publishCompactedRun(state, Set.copyOf(inputRunIds), compacted);
        LOG.debug(
            "Compacted local SSTs: state={}, inputCount={}, outputRunId={}, flushRange=[{},{}]",
            state,
            selected.size(),
            compacted.runId(),
            compacted.minFlushId(),
            compacted.maxFlushId()
        );
        return new CompactionResult.Group(
            state,
            inputRunIds,
            output.snapshot()
        );
    }

    private LocalRun publishFlushedRun(SSTMeta meta) {
        if (meta.state() != SSTState.NEW) {
            throw new IllegalArgumentException("flushed run must be NEW");
        }
        synchronized (runStateMutex) {
            RunState current = runState;
            LocalRun output = new LocalRun(meta);
            List<LocalRun> updated = new ArrayList<>(current.newRuns());
            updated.add(output);
            runState = new RunState(updated, current.sinkedRuns());
            return output;
        }
    }

    private LocalRun publishCompactedRun(SSTState state, Set<Long> inputRunIds, SSTMeta compacted) {
        if (compacted.state() != state) {
            throw new IllegalStateException(
                "compaction output state differs from selected state: expected=" + state
                    + ", actual=" + compacted.state()
            );
        }
        synchronized (runStateMutex) {
            RunState current = runState;
            List<LocalRun> source = current.runs(state);
            List<LocalRun> updated = new ArrayList<>();
            for (LocalRun run : source) {
                if (!inputRunIds.contains(run.runId())) {
                    updated.add(run);
                }
            }
            if (source.size() - updated.size() != inputRunIds.size()) {
                throw new IllegalStateException("selected compaction inputs are no longer maintenance-visible");
            }
            LocalRun output = new LocalRun(compacted);
            updated.add(output);
            runState = state == SSTState.NEW
                ? new RunState(updated, current.sinkedRuns())
                : new RunState(current.newRuns(), updated);
            return output;
        }
    }

    private List<LocalRun> ensureSinkedRuns(Set<Long> sinkedIds, List<SSTMeta> sinkedMetas) {
        if (sinkedMetas.size() != sinkedIds.size()
                || sinkedMetas.stream().anyMatch(meta -> meta.state() != SSTState.SINKED)) {
            throw new IllegalStateException("Sink success did not publish every selected run as SINKED");
        }
        synchronized (runStateMutex) {
            RunState current = runState;
            // A retry may observe each target in either side of the transition. Rebuild the target
            // set exactly once instead of requiring every run to still be NEW.
            Set<Long> visibleIds = new HashSet<>();
            current.newRuns().forEach(run -> visibleIds.add(run.runId()));
            current.sinkedRuns().forEach(run -> visibleIds.add(run.runId()));
            if (!visibleIds.containsAll(sinkedIds)) {
                throw new IllegalStateException("Sink inputs are no longer maintenance-visible");
            }
            List<LocalRun> remainingNew = current.newRuns().stream()
                .filter(run -> !sinkedIds.contains(run.runId()))
                .toList();
            List<LocalRun> published = localRuns(sinkedMetas);
            List<LocalRun> updatedSinked = new ArrayList<>();
            for (LocalRun run : current.sinkedRuns()) {
                if (!sinkedIds.contains(run.runId())) {
                    updatedSinked.add(run);
                }
            }
            updatedSinked.addAll(published);
            runState = new RunState(remainingNew, updatedSinked);
            return published;
        }
    }

    private LocalRun publishEvictedRun(long evictedRunId) {
        synchronized (runStateMutex) {
            RunState current = runState;
            LocalRun evicted = current.sinkedRuns().stream()
                .filter(run -> run.runId() == evictedRunId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "evicted run is not maintenance-visible as SINKED: " + evictedRunId
                ));
            List<LocalRun> remaining = current.sinkedRuns().stream()
                .filter(run -> run.runId() != evictedRunId)
                .toList();
            runState = new RunState(current.newRuns(), remaining);
            return evicted;
        }
    }

    private static List<LocalRun> selectSinkPrefix(SinkSelection selection, List<LocalRun> newRuns) {
        if (newRuns.isEmpty()) {
            return List.of();
        }
        validateContinuousRuns("Sink", newRuns);
        List<LocalRun> selected = new ArrayList<>();
        long selectedBytes = 0;
        for (LocalRun run : newRuns) {
            SSTMeta meta = run.meta();
            if (meta.minSequenceId() <= selection.targetSequenceId()
                    && meta.maxSequenceId() > selection.targetSequenceId()) {
                throw new IllegalStateException(
                    "NEW run crosses Sink target sequence fence: runId=" + run.runId()
                        + ", sequenceRange=[" + meta.minSequenceId() + "," + meta.maxSequenceId() + "]"
                        + ", targetSequenceId=" + selection.targetSequenceId()
                );
            }
            if (meta.maxSequenceId() > selection.targetSequenceId()) {
                break;
            }
            if (!selected.isEmpty()
                    && meta.fileSize() > selection.maxInputBytes() - selectedBytes) {
                break;
            }
            // A single oversized oldest run is allowed so a positive byte limit cannot stall
            // persisted-boundary progress forever.
            selected.add(run);
            selectedBytes = saturatedAdd(selectedBytes, meta.fileSize());
        }
        return List.copyOf(selected);
    }

    private Optional<List<LocalRun>> resolveCompactionSelection(CompactionSelection selection) {
        List<LocalRun> source = runState.runs(selection.state());
        Set<Long> selectedIds = Set.copyOf(selection.inputRunIds());
        List<LocalRun> selected = source.stream()
            .filter(run -> selectedIds.contains(run.runId()))
            .toList();
        if (selected.size() != selection.inputRunIds().size()) {
            return Optional.empty();
        }
        List<Long> orderedIds = selected.stream().map(LocalRun::runId).toList();
        if (!orderedIds.equals(selection.inputRunIds())) {
            throw new IllegalArgumentException("compaction inputs must be ordered from oldest to newest");
        }
        validateContinuousRuns("Compaction", selected);
        long sinkFenceFlushId = selection.state() == SSTState.NEW && sinkFlight.active()
            ? sinkFlight.sinkFenceFlushId()
            : 0;
        for (LocalRun run : selected) {
            SSTMeta sst = run.meta();
            if (sinkFenceFlushId > 0) {
                if (sst.minFlushId() <= sinkFenceFlushId && sst.maxFlushId() > sinkFenceFlushId) {
                    throw new IllegalStateException(
                        "NEW run crosses active Sink fence: run=" + sst.runId()
                            + ", range=[" + sst.minFlushId() + "," + sst.maxFlushId() + "]"
                            + ", fence=" + sinkFenceFlushId
                    );
                }
                if (sst.maxFlushId() <= sinkFenceFlushId) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(selected);
    }

    private static List<LocalRun> resolvePreparedRuns(
            PreparedSinkCommit prepared,
            List<LocalRun> candidates) {
        Set<Long> selectedIds = Set.copyOf(prepared.sstIds());
        List<LocalRun> selected = candidates.stream()
            .filter(run -> selectedIds.contains(run.runId()))
            .toList();
        if (selected.size() != prepared.sstIds().size()) {
            throw new IllegalStateException(
                "pending prepared Sink references missing or replaced NEW runs: batch=" + prepared.batchId()
            );
        }
        List<Long> orderedIds = selected.stream().map(LocalRun::runId).toList();
        if (!orderedIds.equals(prepared.sstIds())) {
            throw new IllegalStateException(
                "pending prepared Sink run order differs from durable metadata: batch=" + prepared.batchId()
            );
        }
        validateContinuousRuns("Prepared Sink", selected);
        List<SSTMeta> selectedMetas = metas(selected);
        if (minSequenceId(selectedMetas) != prepared.minSequenceId()
                || maxSequenceId(selectedMetas) != prepared.maxSequenceId()) {
            throw new IllegalStateException(
                "pending prepared Sink sequence range differs from local runs: batch=" + prepared.batchId()
            );
        }
        return selected;
    }

    private static List<LocalRun> resolveCommittedRuns(
            SinkCommitResult result,
            RunState state) {
        Map<Long, LocalRun> candidates = new TreeMap<>();
        for (LocalRun run : state.newRuns()) {
            candidates.put(run.runId(), run);
        }
        for (LocalRun run : state.sinkedRuns()) {
            if (candidates.put(run.runId(), run) != null) {
                throw new IllegalStateException("run appears in both maintenance states: " + run.runId());
            }
        }
        List<LocalRun> selected = new ArrayList<>(result.sstIds().size());
        for (long runId : result.sstIds()) {
            LocalRun run = candidates.get(runId);
            if (run == null) {
                throw new IllegalStateException(
                    "durable Sink success references missing or replaced local run: batch="
                        + result.batchId()
                        + ", runId="
                        + runId
                );
            }
            selected.add(run);
        }
        validateContinuousRuns("Committed Sink finalization", selected);
        if (maxSequenceId(metas(selected)) != result.persistedSequenceId()) {
            throw new IllegalStateException(
                "durable Sink success sequence boundary differs from local runs: batch=" + result.batchId()
            );
        }
        return List.copyOf(selected);
    }

    private static SinkFlightSnapshot sinkFlightForPrepared(
            PreparedSinkCommit prepared,
            List<LocalRun> selected) {
        return new SinkFlightSnapshot(
            SinkFlightSnapshot.Status.PREPARED_RETRY,
            prepared.batchId(),
            maxFlushId(metas(selected)),
            prepared.minSequenceId(),
            prepared.maxSequenceId()
        );
    }

    private static SinkFlightSnapshot sinkFlightForCommitted(
            SinkCommitResult result,
            List<LocalRun> selected) {
        List<SSTMeta> selectedMetas = metas(selected);
        List<Long> selectedRunIds = selected.stream().map(LocalRun::runId).toList();
        if (!result.sstIds().equals(selectedRunIds)
                || maxSequenceId(selectedMetas) != result.persistedSequenceId()) {
            throw new IllegalStateException(
                "committed Sink result differs from local runs: batch=" + result.batchId()
            );
        }
        return new SinkFlightSnapshot(
            SinkFlightSnapshot.Status.FINALIZING,
            result.batchId(),
            maxFlushId(selectedMetas),
            minSequenceId(selectedMetas),
            result.persistedSequenceId()
        );
    }

    private static long saturatedAdd(long left, long right) {
        if (right > Long.MAX_VALUE - left) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private void maybeFreezeLocked() {
        if (memTables.current().shouldFreeze()) {
            doFreezeLocked();
        }
    }

    /** Caller must hold writeMutex so the WAL fence and MemTable publication are one boundary. */
    private FreezeResult doFreezeLocked() {
        long fenceSequenceId = walManager.lastSequenceId();
        MemTableState currentState = memTables;
        CurMemTable current = currentState.current();
        if (current.estimatedEntryCount() == 0) {
            return FreezeResult.noop(fenceSequenceId);
        }
        CurMemTable replacement = new SkipListCurMemTable(memTableConfig);
        ImmutableMemTable frozen = current.freeze();
        List<ImmutableMemTable> newList = new ArrayList<>(currentState.immutables());
        newList.add(frozen);
        memTables = new MemTableState(replacement, newList);
        LOG.debug("Freeze: immutable count={}", newList.size());
        return new FreezeResult(
            OperationStatus.PROGRESSED,
            fenceSequenceId,
            frozen.minSequenceId(),
            frozen.maxSequenceId(),
            frozen.oldestWriteAtMillis(),
            frozen.estimatedEntryCount(),
            frozen.estimatedSize()
        );
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

    private static long maxFlushId(List<SSTMeta> ssts) {
        return ssts.stream().mapToLong(SSTMeta::maxFlushId).max().orElse(0);
    }

    private static void validateContinuousRuns(String operation, List<LocalRun> runs) {
        long previousMaxFlushId = -1;
        for (LocalRun run : runs) {
            SSTMeta meta = run.meta();
            if (previousMaxFlushId >= 0 && previousMaxFlushId + 1 != meta.minFlushId()) {
                throw new IllegalStateException(
                    operation + " requires continuous local runs: previousMaxFlushId=" + previousMaxFlushId
                        + ", nextRange=[" + meta.minFlushId() + "," + meta.maxFlushId() + "]"
                );
            }
            previousMaxFlushId = meta.maxFlushId();
        }
    }

    private static void validateSinglePendingPrepare(SinkRecoveryState recovery) {
        if (recovery.pendingPrepares().size() > 1) {
            throw new IllegalStateException(
                "V1 supports at most one pending prepared Sink, found " + recovery.pendingPrepares().size()
            );
        }
    }

    private SinkFlightSnapshot sinkFlightFromRecovery(SinkRecoveryState recovery) {
        if (recovery.pendingPrepares().isEmpty()) {
            return SinkFlightSnapshot.idle();
        }
        PreparedSinkCommit prepared = recovery.pendingPrepares().get(0);
        List<LocalRun> candidates = localRuns(storageManager.metas()).stream()
            .filter(run -> run.meta().state() == SSTState.NEW)
            .toList();
        List<LocalRun> selected = resolvePreparedRuns(prepared, candidates);
        return sinkFlightForPrepared(prepared, selected);
    }

    private static SequenceStats sequenceStats(List<ImmutableMemTable> immutables) {
        long totalBytes = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;
        long oldestWriteAtMillis = 0;
        for (ImmutableMemTable im : immutables) {
            totalBytes += im.estimatedSize();
            if (im.minSequenceId() > 0 && (minSequenceId == 0 || im.minSequenceId() < minSequenceId)) {
                minSequenceId = im.minSequenceId();
            }
            maxSequenceId = Math.max(maxSequenceId, im.maxSequenceId());
            if (im.oldestWriteAtMillis() > 0
                    && (oldestWriteAtMillis == 0 || im.oldestWriteAtMillis() < oldestWriteAtMillis)) {
                oldestWriteAtMillis = im.oldestWriteAtMillis();
            }
        }
        return new SequenceStats(totalBytes, minSequenceId, maxSequenceId, oldestWriteAtMillis);
    }

    private static RunStats runStats(List<LocalRun> runs) {
        long totalBytes = 0;
        long totalRows = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;
        long oldestWriteAtMillis = 0;
        for (LocalRun run : runs) {
            SSTMeta sst = run.meta();
            totalBytes += sst.fileSize();
            totalRows += sst.entryCount();
            if (sst.minSequenceId() > 0 && (minSequenceId == 0 || sst.minSequenceId() < minSequenceId)) {
                minSequenceId = sst.minSequenceId();
            }
            maxSequenceId = Math.max(maxSequenceId, sst.maxSequenceId());
            if (run.oldestWriteAtMillis() > 0
                    && (oldestWriteAtMillis == 0 || run.oldestWriteAtMillis() < oldestWriteAtMillis)) {
                oldestWriteAtMillis = run.oldestWriteAtMillis();
            }
        }
        return new RunStats(totalBytes, totalRows, minSequenceId, maxSequenceId, oldestWriteAtMillis);
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
        long lastFlushedSequenceId,
        long maxRecoveredSequenceId
    ) {}

    private record SequenceStats(
        long totalBytes,
        long minSequenceId,
        long maxSequenceId,
        long oldestWriteAtMillis
    ) {}

    private record RunStats(
        long totalBytes,
        long totalRows,
        long minSequenceId,
        long maxSequenceId,
        long oldestWriteAtMillis
    ) {}

    private record MemTableState(CurMemTable current, List<ImmutableMemTable> immutables) {
        private MemTableState {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(immutables, "immutables must not be null");
            immutables = List.copyOf(immutables);
        }
    }

    /** In-process ownership of one Storage-published SST whose Flush handoff is not complete yet. */
    private record FlushFlight(ImmutableMemTable source, SSTMeta output) {
        private FlushFlight {
            Objects.requireNonNull(source, "source must not be null");
            Objects.requireNonNull(output, "output must not be null");
        }
    }

    /**
     * Immutable maintenance-visible SST view. It deliberately advances after the storage mutation:
     * Flush publishes a run only after its boundary and MemTable handoff complete, while
     * Sink/compact/evict publish one delta after their storage operation completes.
     */
    private record RunState(List<LocalRun> newRuns, List<LocalRun> sinkedRuns) {
        private RunState {
            newRuns = normalize(newRuns, SSTState.NEW);
            sinkedRuns = normalize(sinkedRuns, SSTState.SINKED);
            Set<Long> runIds = new HashSet<>();
            for (LocalRun run : newRuns) {
                if (!runIds.add(run.runId())) {
                    throw new IllegalArgumentException("duplicate NEW run id: " + run.runId());
                }
            }
            for (LocalRun run : sinkedRuns) {
                if (!runIds.add(run.runId())) {
                    throw new IllegalArgumentException("run id appears in both SST states: " + run.runId());
                }
            }
        }

        private static RunState empty() {
            return new RunState(List.of(), List.of());
        }

        private static RunState fromMetas(List<SSTMeta> metas) {
            List<LocalRun> runs = localRuns(metas);
            return new RunState(
                runs.stream().filter(run -> run.meta().state() == SSTState.NEW).toList(),
                runs.stream().filter(run -> run.meta().state() == SSTState.SINKED).toList()
            );
        }

        private List<LocalRun> runs(SSTState state) {
            return state == SSTState.NEW ? newRuns : sinkedRuns;
        }

        private static List<LocalRun> normalize(List<LocalRun> runs, SSTState expectedState) {
            Objects.requireNonNull(runs, "runs must not be null");
            for (LocalRun run : runs) {
                Objects.requireNonNull(run, "run must not be null");
                if (run.meta().state() != expectedState) {
                    throw new IllegalArgumentException(
                        "run has unexpected state: runId=" + run.runId()
                            + ", expected=" + expectedState
                            + ", actual=" + run.meta().state()
                    );
                }
            }
            return runs.stream()
                .sorted(
                    Comparator.comparingLong((LocalRun run) -> run.meta().maxFlushId())
                        .thenComparingLong(run -> run.meta().minFlushId())
                )
                .toList();
        }
    }
}
