package org.qwh.pms.core.storage;

import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class FileLocalStorageManager implements LocalStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileLocalStorageManager.class);

    private final Path dir;
    private final FlushBoundaryStore flushBoundaryStore;
    private final SSTMetaStore sstMetaStore;
    private final FileDeleter fileDeleter;
    private final Object flushBoundaryMutex = new Object();
    private final Object flushWriteMutex = new Object();
    private final ConcurrentSkipListMap<Long, SSTMeta> metas = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<Long, SSTReaderRef> readers = new ConcurrentHashMap<>();
    private final Map<Long, Integer> activeReadEpochs = new HashMap<>();
    private final List<RetiredSST> retiredSSTs = new ArrayList<>();
    private final AtomicLong nextRunId = new AtomicLong(1);
    private final AtomicLong nextFlushId = new AtomicLong(1);
    private long currentEpoch = 1;
    private volatile long lastFlushedSequenceId;

    public FileLocalStorageManager(StorageConfig config) {
        this(config, path -> Files.deleteIfExists(path));
    }

    FileLocalStorageManager(StorageConfig config, FileDeleter fileDeleter) {
        if (config.dir() == null) {
            throw new IllegalArgumentException("storage dir must not be null");
        }
        this.dir = Path.of(config.dir());
        this.flushBoundaryStore = new FlushBoundaryStore(dir);
        this.sstMetaStore = new SSTMetaStore(dir);
        this.fileDeleter = Objects.requireNonNull(fileDeleter, "fileDeleter must not be null");
    }

    public synchronized void init() throws IOException {
        init(0);
    }

    /**
     * Initializes the local SST view using the already durable Paimon sequence boundary.
     */
    public synchronized void init(long lastPersistedSequenceId) throws IOException {
        Files.createDirectories(dir);
        sstMetaStore.init();
        lastFlushedSequenceId = flushBoundaryStore.load();
        SSTRecoveryPlanner.RecoveryPlan plan =
            new SSTRecoveryPlanner(dir, sstMetaStore)
                .recover(lastFlushedSequenceId, lastPersistedSequenceId);
        for (SSTMeta meta : plan.visibleMetas()) {
            SSTMeta stored = sstMetaStore.load(sstMetaStore.metaPath(meta));
            if (stored.state() != meta.state()) {
                // SinkMeta's persisted sequence boundary is authoritative over the observable
                // state stored in an older SST metadata file.
                sstMetaStore.save(meta);
            }
            SSTReaderRef reader = new SSTReaderRef(SSTReader.open(meta));
            putVisibleMeta(meta);
            readers.put(meta.runId(), reader);
        }
        nextRunId.set(plan.nextRunId());
        nextFlushId.set(plan.nextFlushId());
        for (SSTMeta obsolete : plan.cleanupMetas()) {
            if (!deleteSSTFilesQuietly(obsolete)) {
                LOG.warn("Recovered SST garbage will be retried on the next start: {}", obsolete.path());
            }
        }
        for (Path dataPath : plan.cleanupDataFiles()) {
            try {
                fileDeleter.deleteIfExists(dataPath);
            } catch (IOException e) {
                LOG.warn(
                    "Recovered SST data without metadata will be retried on the next start: {}",
                    dataPath,
                    e
                );
            }
        }
    }

    public synchronized void close() {
        activeReadEpochs.clear();
        reclaimRetiredSSTs();
        for (SSTReaderRef holder : readers.values()) {
            holder.close();
        }
        readers.clear();
    }

    public synchronized List<SSTMeta> metas() {
        return metas.values().stream()
            .sorted(Comparator.comparingLong(SSTMeta::maxFlushId).thenComparingLong(SSTMeta::minFlushId))
            .toList();
    }

    public synchronized List<SSTMeta> metas(SSTState state) {
        return metas.values().stream()
            .filter(meta -> meta.state() == state)
            .sorted(Comparator.comparingLong(SSTMeta::maxFlushId).thenComparingLong(SSTMeta::minFlushId))
            .toList();
    }

    public long lastFlushedSequenceId() {
        return lastFlushedSequenceId;
    }

    public void persistFlushedSequenceId(long sequenceId) {
        synchronized (flushBoundaryMutex) {
            if (sequenceId <= lastFlushedSequenceId) {
                return;
            }
            try {
                flushBoundaryStore.save(sequenceId);
                lastFlushedSequenceId = sequenceId;
            } catch (IOException e) {
                throw new RuntimeException("persist flush boundary failed", e);
            }
        }
    }

    /**
     * Reconciles recovered Sink state before the director starts serving requests.
     */
    public synchronized void applyPersistedSequenceId(long persistedSequenceId) {
        for (SSTMeta meta : List.copyOf(metas.values())) {
            SSTState target;
            if (meta.maxSequenceId() <= persistedSequenceId) {
                target = SSTState.SINKED;
            } else if (meta.minSequenceId() > persistedSequenceId) {
                target = SSTState.NEW;
            } else {
                throw new IllegalStateException(
                    "SST crosses the durable Sink boundary: runId="
                        + meta.runId()
                        + ", sequenceRange=["
                        + meta.minSequenceId()
                        + ","
                        + meta.maxSequenceId()
                        + "], persistedSequenceId="
                        + persistedSequenceId
                );
            }
            if (meta.state() != target) {
                updateState(meta, target);
            }
        }
    }

    public List<SSTMeta> markSinked(List<SSTMeta> toMark) {
        Objects.requireNonNull(toMark, "toMark must not be null");
        List<SSTMeta> updated;
        synchronized (this) {
            // Resolve every requested run exactly. Silently dropping one would allow a durable
            // Paimon success to be published with an incomplete local SINKED set.
            List<SSTMeta> targets = new ArrayList<>(toMark.size());
            for (SSTMeta requested : toMark) {
                SSTMeta current = metas.get(requested.runId());
                if (current == null) {
                    throw new IllegalStateException(
                        "SST selected for Sink finalization is no longer visible: runId=" + requested.runId()
                    );
                }
                targets.add(current.withPathAndState(current.path(), SSTState.SINKED));
            }
            updated = List.copyOf(targets);
        }

        try {
            // Disk files are persisted one by one outside the visibility monitor. A failed attempt
            // may leave a durable prefix, but memory is unchanged and retrying the same target
            // state simply overwrites that prefix with identical SINKED metadata.
            for (SSTMeta meta : updated) {
                sstMetaStore.save(meta);
            }
        } catch (IOException e) {
            throw new RuntimeException("persist SINKED SST metadata failed", e);
        }

        synchronized (this) {
            // Publish only after the whole metadata batch is durable.
            validateStatePublicationInputs(updated);
            for (SSTMeta meta : updated) {
                putVisibleMeta(meta);
            }
            return updated;
        }
    }

    @Override
    public SSTMeta flushToSST(ImmutableMemTable memTable) {
        synchronized (flushWriteMutex) {
            long runId = nextRunId.getAndIncrement();
            long flushId = nextFlushId.get();
            SSTReaderRef preparedReader = null;
            boolean published = false;
            try {
                SSTMeta meta = new SSTWriter(
                    dir,
                    SSTFormat.DEFAULT_BLOCK_SIZE,
                    SSTFormat.DEFAULT_RESTART_INTERVAL
                ).write(runId, flushId, memTable);
                sstMetaStore.save(meta);
                preparedReader = new SSTReaderRef(SSTReader.open(meta));
                synchronized (this) {
                    publishNewVisibleMeta(meta, preparedReader);
                    // Do not consume the logical ID until the complete SST is visible.
                    nextFlushId.incrementAndGet();
                    published = true;
                }
                return meta;
            } catch (IOException e) {
                if (!published) {
                    cleanupUnpublishedSST(runId, flushId, flushId, preparedReader);
                }
                throw new RuntimeException("flush to SST failed", e);
            } catch (RuntimeException e) {
                if (!published) {
                    cleanupUnpublishedSST(runId, flushId, flushId, preparedReader);
                }
                throw e;
            }
        }
    }

    @Override
    public SSTReadSnapshot readSnapshot(List<SSTMeta> metas) {
        Objects.requireNonNull(metas, "metas must not be null");
        if (metas.isEmpty()) {
            return EmptySSTReadSnapshot.INSTANCE;
        }
        long epoch = beginReadEpoch();
        try {
            return new FileSSTReadSnapshot(epoch, List.copyOf(metas));
        } catch (RuntimeException e) {
            endReadEpoch(epoch);
            throw e;
        }
    }

    @Override
    public synchronized SSTReadSnapshot readVisibleSnapshot() {
        List<SSTMeta> visible = metas.values().stream()
            .sorted(Comparator.comparingLong(SSTMeta::maxFlushId).thenComparingLong(SSTMeta::minFlushId))
            .toList();
        return readSnapshot(visible);
    }

    @Override
    public SSTMeta compactSSTs(List<SSTMeta> metas) {
        if (metas == null || metas.size() < 2) {
            throw new IllegalArgumentException("at least two SSTs are required for compaction");
        }
        List<SSTMeta> inputs = metas.stream()
            .sorted(Comparator.comparingLong(SSTMeta::minFlushId).thenComparingLong(SSTMeta::maxFlushId))
            .toList();
        validateCompactInputs(inputs);

        List<SSTEntryIterator> iterators = new ArrayList<>(inputs.size());
        long outputRunId = nextRunId.getAndIncrement();
        long outputMinFlushId = inputs.get(0).minFlushId();
        long outputMaxFlushId = inputs.get(inputs.size() - 1).maxFlushId();
        SSTReaderRef preparedReader = null;
        boolean published = false;
        try {
            long expectedEntries = inputs.stream().mapToLong(SSTMeta::entryCount).sum();
            long minSequenceId = inputs.stream().mapToLong(SSTMeta::minSequenceId).min().orElse(0);
            long maxSequenceId = inputs.stream().mapToLong(SSTMeta::maxSequenceId).max().orElse(0);
            long oldestWriteAtMillis = inputs.stream().mapToLong(SSTMeta::oldestWriteAtMillis).min().orElse(0);
            SSTMeta output;
            try (SSTReadSnapshot snapshot = readSnapshot(inputs)) {
                for (SSTMeta meta : inputs) {
                    iterators.add(snapshot.openIterator(meta));
                }
                try (SSTEntryIterator merged = new MergedSSTEntryIterator(iterators)) {
                    output = new SSTWriter(
                        dir,
                        SSTFormat.DEFAULT_BLOCK_SIZE,
                        SSTFormat.DEFAULT_RESTART_INTERVAL
                    ).write(
                        outputRunId,
                        outputMinFlushId,
                        outputMaxFlushId,
                        inputs.get(0).state(),
                        merged,
                        expectedEntries,
                        minSequenceId,
                        maxSequenceId,
                        oldestWriteAtMillis
                    );
                }
            }
            sstMetaStore.save(output);
            preparedReader = new SSTReaderRef(SSTReader.open(output));
            synchronized (this) {
                validateCompactionPublication(inputs, output);
                for (SSTMeta input : inputs) {
                    this.metas.remove(input.runId());
                }
                putVisibleMeta(output);
                readers.put(output.runId(), preparedReader);
                retireSSTs(inputs);
                published = true;
            }
            return output;
        } catch (IOException e) {
            if (!published) {
                cleanupUnpublishedSST(
                    outputRunId,
                    outputMinFlushId,
                    outputMaxFlushId,
                    preparedReader
                );
            }
            throw new RuntimeException("compact SSTs failed", e);
        } catch (RuntimeException e) {
            closeAll(iterators, e);
            if (!published) {
                cleanupUnpublishedSST(
                    outputRunId,
                    outputMinFlushId,
                    outputMaxFlushId,
                    preparedReader
                );
            }
            throw e;
        }
    }

    @Override
    public synchronized void deleteSST(SSTMeta meta) {
        metas.remove(meta.runId());
        retireSSTs(List.of(meta));
    }

    private void deleteSSTFiles(SSTMeta meta) {
        try {
            // Keep data-first syscall ordering so an ordinary failure leaves recoverable metadata.
            // V1 deliberately avoids directory fsync on this best-effort cache cleanup path.
            fileDeleter.deleteIfExists(meta.path());
            fileDeleter.deleteIfExists(sstMetaStore.metaPath(meta));
        } catch (IOException e) {
            throw new RuntimeException("delete SST failed: " + meta.path(), e);
        }
    }

    private void updateState(SSTMeta meta, SSTState target) {
        SSTMeta updated = meta.withPathAndState(meta.path(), target);
        try {
            sstMetaStore.save(updated);
            putVisibleMeta(updated);
        } catch (IOException e) {
            throw new RuntimeException("persist SST metadata failed: " + sstMetaStore.metaPath(meta), e);
        }
    }

    /**
     * Publishes a fully prepared data/meta/reader tuple. Callers must hold this manager's monitor.
     */
    private void publishNewVisibleMeta(SSTMeta meta, SSTReaderRef reader) {
        if (metas.containsKey(meta.runId()) || readers.containsKey(meta.runId())) {
            throw new IllegalStateException("duplicate SST runId during publication: " + meta.runId());
        }
        putVisibleMeta(meta);
        readers.put(meta.runId(), reader);
    }

    private void validateStatePublicationInputs(List<SSTMeta> updated) {
        for (SSTMeta target : updated) {
            SSTMeta current = metas.get(target.runId());
            if (current == null
                    || !current.withPathAndState(current.path(), target.state()).equals(target)) {
                throw new IllegalStateException(
                    "SST changed while its state metadata was being persisted: runId=" + target.runId()
                );
            }
        }
    }

    private void validateCompactionPublication(List<SSTMeta> inputs, SSTMeta output) {
        Set<Long> inputRunIds = Set.copyOf(inputs.stream().map(SSTMeta::runId).toList());
        for (SSTMeta input : inputs) {
            if (!input.equals(metas.get(input.runId()))) {
                throw new IllegalStateException(
                    "SST changed while compaction output was being prepared: runId=" + input.runId()
                );
            }
        }
        if (metas.containsKey(output.runId()) || readers.containsKey(output.runId())) {
            throw new IllegalStateException("duplicate compacted SST runId: " + output.runId());
        }
        for (SSTMeta existing : metas.values()) {
            if (!inputRunIds.contains(existing.runId()) && overlaps(existing, output)) {
                throw new IllegalStateException(
                    "compaction output overlaps a non-input SST: output="
                        + output.path()
                        + ", existing="
                        + existing.path()
                );
            }
        }
    }

    private void cleanupUnpublishedSST(
            long runId,
            long minFlushId,
            long maxFlushId,
            SSTReaderRef preparedReader) {
        if (preparedReader != null) {
            preparedReader.close();
        }
        Path dataPath = SSTWriter.pathFor(dir, minFlushId, maxFlushId, SSTState.NEW);
        Path metaPath = sstMetaStore.metaPath(minFlushId, maxFlushId);
        try {
            Files.deleteIfExists(dataPath);
        } catch (IOException e) {
            LOG.warn(
                "Failed to clean unpublished SST data file: runId={}, path={}",
                runId,
                dataPath,
                e
            );
        }
        try {
            Files.deleteIfExists(metaPath);
        } catch (IOException e) {
            LOG.warn(
                "Failed to clean unpublished SST metadata: runId={}, path={}",
                runId,
                metaPath,
                e
            );
        }
    }

    private SSTReader readerFor(SSTMeta meta) {
        SSTReaderRef holder = readers.get(meta.runId());
        if (holder != null && holder.matches(meta)) {
            return holder.reader();
        }
        String cachedReader = holder == null
            ? "none"
            : "runId=" + holder.reader().meta().runId() + ", path=" + holder.reader().meta().path();
        String message = "Expected SST reader is missing from the reader cache: runId="
            + meta.runId()
            + ", flushRange=["
            + meta.minFlushId()
            + ","
            + meta.maxFlushId()
            + "], path="
            + meta.path()
            + ", dataFileExists="
            + Files.exists(meta.path())
            + ", cachedReader={"
            + cachedReader
            + "}";
        LOG.error("PMS_STORAGE_INVARIANT {}", message);
        throw new IllegalStateException(message);
    }

    private synchronized long beginReadEpoch() {
        long epoch = currentEpoch;
        activeReadEpochs.merge(epoch, 1, Integer::sum);
        return epoch;
    }

    private synchronized void endReadEpoch(long epoch) {
        Integer count = activeReadEpochs.get(epoch);
        if (count == null) {
            throw new IllegalStateException("read epoch released more than once: " + epoch);
        }
        if (count == 1) {
            activeReadEpochs.remove(epoch);
        } else {
            activeReadEpochs.put(epoch, count - 1);
        }
        reclaimRetiredSSTs();
    }

    private void retireSSTs(List<SSTMeta> metas) {
        if (metas.isEmpty()) {
            return;
        }
        long retireEpoch = ++currentEpoch;
        for (SSTMeta meta : metas) {
            retiredSSTs.add(new RetiredSST(meta, retireEpoch));
        }
        reclaimRetiredSSTs();
    }

    private void reclaimRetiredSSTs() {
        // TODO(pms-storage): detach reclaimable reader/file handles under this monitor and perform
        // close/unlink outside it. A dedicated cleanup queue may later remove the remaining tail
        // latency from the query that releases the last old read epoch.
        long minActiveEpoch = activeReadEpochs.keySet().stream()
            .mapToLong(Long::longValue)
            .min()
            .orElse(Long.MAX_VALUE);
        for (int i = 0; i < retiredSSTs.size(); ) {
            RetiredSST retired = retiredSSTs.get(i);
            if (minActiveEpoch < retired.retireEpoch()) {
                i++;
                continue;
            }
            SSTReaderRef holder = readers.remove(retired.meta().runId());
            if (holder != null) {
                holder.close();
            }
            if (deleteSSTFilesQuietly(retired.meta())) {
                // A partial data-first deletion remains here and is retried idempotently.
                retiredSSTs.remove(i);
            } else {
                i++;
            }
        }
    }

    private boolean deleteSSTFilesQuietly(SSTMeta meta) {
        try {
            deleteSSTFiles(meta);
            return true;
        } catch (RuntimeException e) {
            LOG.warn("Failed to delete retired SST files: {}", meta.path(), e);
            return false;
        }
    }

    private static final class SSTReaderRef {
        private final SSTReader reader;

        private SSTReaderRef(SSTReader reader) {
            this.reader = reader;
        }

        private SSTReader reader() {
            return reader;
        }

        private boolean matches(SSTMeta meta) {
            return reader.meta().runId() == meta.runId() && reader.meta().path().equals(meta.path());
        }

        private void close() {
            reader.close();
        }
    }

    private enum EmptySSTReadSnapshot implements SSTReadSnapshot {
        INSTANCE;

        @Override
        public List<SSTMeta> metas() {
            return List.of();
        }

        @Override
        public Optional<Value> get(SSTMeta meta, Key key) {
            throw new IllegalArgumentException("empty SST read snapshot has no SST readers");
        }

        @Override
        public SSTEntryIterator openIterator(SSTMeta meta) {
            throw new IllegalArgumentException("empty SST read snapshot has no SST readers");
        }

        @Override
        public SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
            throw new IllegalArgumentException("empty SST read snapshot has no SST readers");
        }

        @Override
        public void close() {
        }
    }

    private final class FileSSTReadSnapshot implements SSTReadSnapshot {
        private final long epoch;
        private final List<SSTMeta> metas;
        private boolean closed;

        private FileSSTReadSnapshot(long epoch, List<SSTMeta> metas) {
            this.epoch = epoch;
            this.metas = metas;
        }

        @Override
        public List<SSTMeta> metas() {
            return metas;
        }

        @Override
        public Optional<Value> get(SSTMeta meta, Key key) {
            ensureOpen();
            try {
                return readerFor(meta).get(key);
            } catch (IOException e) {
                throw new RuntimeException("SST read failed: " + meta.path(), e);
            }
        }

        @Override
        public SSTEntryIterator openIterator(SSTMeta meta) {
            ensureOpen();
            return readerFor(meta).iterator();
        }

        @Override
        public SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
            ensureOpen();
            return readerFor(meta).iterator(startInclusive, endExclusive);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            endReadEpoch(epoch);
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("SST read snapshot is closed");
            }
        }
    }

    private record RetiredSST(SSTMeta meta, long retireEpoch) {}

    @FunctionalInterface
    interface FileDeleter {
        void deleteIfExists(Path path) throws IOException;
    }

    private void putVisibleMeta(SSTMeta meta) {
        for (SSTMeta existing : metas.values()) {
            if (existing.runId() != meta.runId() && overlaps(existing, meta)) {
                throw new IllegalArgumentException(
                    "overlapping SST flush ranges: " + existing.path() + " and " + meta.path()
                );
            }
        }
        metas.put(meta.runId(), meta);
    }

    private static boolean overlaps(SSTMeta left, SSTMeta right) {
        return left.minFlushId() <= right.maxFlushId() && right.minFlushId() <= left.maxFlushId();
    }

    private static void validateCompactInputs(List<SSTMeta> inputs) {
        SSTState state = inputs.get(0).state();
        for (int i = 0; i < inputs.size(); i++) {
            SSTMeta meta = inputs.get(i);
            if (meta.state() != state) {
                throw new IllegalArgumentException("cannot compact SSTs with different states");
            }
            if (i > 0 && inputs.get(i - 1).maxFlushId() + 1 != meta.minFlushId()) {
                throw new IllegalArgumentException("can only compact continuous flush ranges");
            }
        }
    }

    private static void closeAll(List<SSTEntryIterator> iterators, RuntimeException owner) {
        for (SSTEntryIterator iterator : iterators) {
            try {
                iterator.close();
            } catch (RuntimeException e) {
                owner.addSuppressed(e);
            }
        }
    }

    private static final class MergedSSTEntryIterator implements SSTEntryIterator {
        private final List<SSTEntryIterator> inputs;
        private final PriorityQueue<CursorEntry> heap;
        private Entry next;
        private boolean closed;

        private MergedSSTEntryIterator(List<SSTEntryIterator> inputs) {
            this.inputs = List.copyOf(inputs);
            this.heap = new PriorityQueue<>(
                Comparator.comparing((CursorEntry cursor) -> cursor.entry().key())
                    .thenComparingInt(CursorEntry::sourceIndex)
            );
            for (int i = 0; i < this.inputs.size(); i++) {
                advance(i);
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (next == null) {
                next = readNext();
                if (next == null) {
                    close();
                }
            }
            return next != null;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            heap.clear();
            for (SSTEntryIterator input : inputs) {
                input.close();
            }
        }

        private Entry readNext() {
            if (heap.isEmpty()) {
                return null;
            }

            CursorEntry first = heap.poll();
            Key key = first.entry().key();
            Entry newest = first.entry();
            advance(first.sourceIndex());

            while (!heap.isEmpty() && heap.peek().entry().key().compareTo(key) == 0) {
                CursorEntry candidate = heap.poll();
                if (candidate.entry().value().sequenceId() > newest.value().sequenceId()) {
                    newest = candidate.entry();
                }
                advance(candidate.sourceIndex());
            }
            return newest;
        }

        private void advance(int sourceIndex) {
            SSTEntryIterator input = inputs.get(sourceIndex);
            if (input.hasNext()) {
                heap.add(new CursorEntry(sourceIndex, input.next()));
            }
        }
    }

    private record CursorEntry(int sourceIndex, Entry entry) {}
}
