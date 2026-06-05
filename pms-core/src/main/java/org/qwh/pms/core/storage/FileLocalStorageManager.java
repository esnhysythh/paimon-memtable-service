package org.qwh.pms.core.storage;

import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
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
    private final ConcurrentSkipListMap<Long, SSTMeta> metas = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<Long, SSTReaderRef> readers = new ConcurrentHashMap<>();
    private final Map<Long, Integer> activeReadEpochs = new HashMap<>();
    private final List<RetiredSST> retiredSSTs = new ArrayList<>();
    private final AtomicLong nextRunId = new AtomicLong(1);
    private final AtomicLong nextFlushId = new AtomicLong(1);
    private long currentEpoch = 1;
    private long lastFlushedSequenceId;

    public FileLocalStorageManager(StorageConfig config) {
        if (config.dir() == null) {
            throw new IllegalArgumentException("storage dir must not be null");
        }
        this.dir = Path.of(config.dir());
        this.flushBoundaryStore = new FlushBoundaryStore(dir);
        this.sstMetaStore = new SSTMetaStore(dir);
    }

    public synchronized void init() throws IOException {
        Files.createDirectories(dir);
        sstMetaStore.init();
        lastFlushedSequenceId = flushBoundaryStore.load();
        long maxRunId = 0;
        long maxFlushId = 0;
        List<SSTMeta> recoveredMetas = new ArrayList<>();
        for (Path path : sstMetaStore.listMetaFiles()) {
            try {
                SSTMeta meta = sstMetaStore.load(path);
                maxRunId = Math.max(maxRunId, meta.runId());
                maxFlushId = Math.max(maxFlushId, meta.maxFlushId());
                if (!Files.exists(meta.path())) {
                    throw new IOException(
                        "SST files are missing after flush boundary was persisted: " + meta.path()
                    );
                }
                SSTMeta actual = SSTReader.readMeta(meta.path(), meta.state());
                validateMetaMatchesFile(meta, actual);
                if (meta.maxSequenceId() > lastFlushedSequenceId) {
                    LOG.warn(
                        "Ignore orphan SST beyond flush boundary: path={}, maxSequenceId={}, lastFlushedSequenceId={}",
                        meta.path(),
                        meta.maxSequenceId(),
                        lastFlushedSequenceId
                    );
                    continue;
                }
                recoveredMetas.add(meta);
            } catch (IOException | RuntimeException e) {
                if (lastFlushedSequenceId > 0) {
                    if (e instanceof IOException && e.getMessage() != null
                        && e.getMessage().contains("SST files are missing after flush boundary was persisted")) {
                        throw (IOException) e;
                    }
                    throw new IOException(
                        "SST metadata is corrupt after flush boundary was persisted: " + path,
                        e
                    );
                }
                LOG.warn("Skip corrupted SST metadata during init: {}", path, e);
            }
        }
        for (SSTMeta meta : reconcileVisibleMetas(recoveredMetas)) {
            putVisibleMeta(meta);
            readers.put(meta.runId(), new SSTReaderRef(SSTReader.open(meta)));
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sst-*.sst")) {
            for (Path path : stream) {
                long[] flushRange = parseFlushRange(path);
                maxFlushId = Math.max(maxFlushId, flushRange[1]);
                if (hasMetaForRange(flushRange[0], flushRange[1])) {
                    continue;
                }
                if (isRangeCoveredByVisibleMetas(flushRange[0], flushRange[1])) {
                    LOG.warn("Ignore compacted SST data file covered by visible metadata: {}", path);
                    continue;
                }
                SSTMeta actual = SSTReader.readMeta(path, stateFromPath(path));
                if (actual.maxSequenceId() > lastFlushedSequenceId) {
                    LOG.warn(
                        "Ignore orphan SST beyond flush boundary: path={}, maxSequenceId={}, lastFlushedSequenceId={}",
                        path,
                        actual.maxSequenceId(),
                        lastFlushedSequenceId
                    );
                    continue;
                }
                if (lastFlushedSequenceId > 0) {
                    throw new IOException(
                        "SST metadata file is missing after flush boundary was persisted: " + path
                    );
                }
                LOG.warn("Ignore SST without metadata before flush boundary: {}", path);
            }
        }
        validateFlushBoundaryCoveredBySST();
        validateNoOverlappingRuns();
        nextRunId.set(maxRunId + 1);
        nextFlushId.set(maxFlushId + 1);
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

    public synchronized long lastFlushedSequenceId() {
        return lastFlushedSequenceId;
    }

    public synchronized void persistFlushedSequenceId(long sequenceId) {
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

    public synchronized void applySinkedSSTIds(Set<Long> sinkedIds, long persistedSequenceId) {
        for (SSTMeta meta : List.copyOf(metas.values())) {
            SSTState target = sinkedIds.contains(meta.runId()) || meta.maxSequenceId() <= persistedSequenceId
                ? SSTState.SINKED
                : SSTState.NEW;
            if (meta.state() != target) {
                updateState(meta, target);
            }
        }
    }

    public synchronized List<SSTMeta> markSinked(List<SSTMeta> toMark) {
        for (SSTMeta meta : toMark) {
            SSTMeta current = metas.get(meta.runId());
            if (current != null) {
                updateState(current, SSTState.SINKED);
            }
        }
        return metas(SSTState.SINKED);
    }

    @Override
    public synchronized SSTMeta flushToSST(ImmutableMemTable memTable) {
        try {
            long flushId = nextFlushId.getAndIncrement();
            SSTMeta meta = new SSTWriter(
                dir,
                SSTFormat.DEFAULT_BLOCK_SIZE,
                SSTFormat.DEFAULT_RESTART_INTERVAL
            ).write(nextRunId.getAndIncrement(), flushId, memTable);
            sstMetaStore.save(meta);
            putVisibleMeta(meta);
            readers.put(meta.runId(), new SSTReaderRef(SSTReader.open(meta)));
            return meta;
        } catch (IOException e) {
            throw new RuntimeException("flush to SST failed", e);
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
    public SSTMeta compactSSTs(List<SSTMeta> metas) {
        if (metas == null || metas.size() < 2) {
            throw new IllegalArgumentException("at least two SSTs are required for compaction");
        }
        List<SSTMeta> inputs = metas.stream()
            .sorted(Comparator.comparingLong(SSTMeta::minFlushId).thenComparingLong(SSTMeta::maxFlushId))
            .toList();
        validateCompactInputs(inputs);

        List<SSTEntryIterator> iterators = new ArrayList<>(inputs.size());
        try {
            long expectedEntries = inputs.stream().mapToLong(SSTMeta::entryCount).sum();
            long minSequenceId = inputs.stream().mapToLong(SSTMeta::minSequenceId).min().orElse(0);
            long maxSequenceId = inputs.stream().mapToLong(SSTMeta::maxSequenceId).max().orElse(0);
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
                        nextRunId.getAndIncrement(),
                        inputs.get(0).minFlushId(),
                        inputs.get(inputs.size() - 1).maxFlushId(),
                        inputs.get(0).state(),
                        merged,
                        expectedEntries,
                        minSequenceId,
                        maxSequenceId
                    );
                }
            }
            synchronized (this) {
                sstMetaStore.save(output);
                for (SSTMeta input : inputs) {
                    this.metas.remove(input.runId());
                }
                putVisibleMeta(output);
                readers.put(output.runId(), new SSTReaderRef(SSTReader.open(output)));
                retireSSTs(inputs);
            }
            return output;
        } catch (IOException e) {
            throw new RuntimeException("compact SSTs failed", e);
        } catch (RuntimeException e) {
            closeAll(iterators, e);
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
            deleteSSTDataFile(meta);
            deleteSSTMetadata(meta);
        } catch (IOException e) {
            throw new RuntimeException("delete SST failed: " + meta.path(), e);
        }
    }

    private void deleteSSTDataFile(SSTMeta meta) throws IOException {
        Files.deleteIfExists(meta.path());
    }

    private void deleteSSTMetadata(SSTMeta meta) throws IOException {
        Files.deleteIfExists(sstMetaStore.metaPath(meta));
    }

    @Override
    public synchronized Optional<SSTMeta> evictOldestSinkedSST() {
        Optional<SSTMeta> oldest = metas.values().stream()
            .filter(meta -> meta.state() == SSTState.SINKED)
            .min(Comparator.comparingLong(SSTMeta::maxFlushId).thenComparingLong(SSTMeta::minFlushId));
        oldest.ifPresent(this::deleteSST);
        return oldest;
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

    private SSTReader readerFor(SSTMeta meta) throws IOException {
        SSTReaderRef holder = readers.get(meta.runId());
        if (holder != null && holder.matches(meta)) {
            return holder.reader();
        }
        synchronized (this) {
            holder = readers.get(meta.runId());
            if (holder != null && holder.matches(meta)) {
                return holder.reader();
            }
            SSTMeta effectiveMeta = metas.getOrDefault(meta.runId(), meta);
            holder = new SSTReaderRef(SSTReader.open(effectiveMeta));
            readers.put(effectiveMeta.runId(), holder);
            return holder.reader();
        }
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
            retiredSSTs.remove(i);
            SSTReaderRef holder = readers.remove(retired.meta().runId());
            if (holder != null) {
                holder.close();
            }
            deleteSSTFilesQuietly(retired.meta());
        }
    }

    private void deleteSSTFilesQuietly(SSTMeta meta) {
        try {
            deleteSSTFiles(meta);
        } catch (RuntimeException e) {
            LOG.warn("Failed to delete retired SST files: {}", meta.path(), e);
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
            try {
                return readerFor(meta).iterator();
            } catch (IOException e) {
                throw new RuntimeException("SST iterator open failed: " + meta.path(), e);
            }
        }

        @Override
        public SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
            ensureOpen();
            try {
                return readerFor(meta).iterator(startInclusive, endExclusive);
            } catch (IOException e) {
                throw new RuntimeException("SST iterator open failed: " + meta.path(), e);
            }
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

    private boolean hasMetaForRange(long minFlushId, long maxFlushId) {
        return metas.values().stream()
            .anyMatch(meta -> meta.minFlushId() == minFlushId && meta.maxFlushId() == maxFlushId)
            || Files.exists(sstMetaStore.metaPath(minFlushId, maxFlushId));
    }

    private boolean isRangeCoveredByVisibleMetas(long minFlushId, long maxFlushId) {
        long next = minFlushId;
        for (SSTMeta meta : metas.values().stream()
            .sorted(Comparator.comparingLong(SSTMeta::minFlushId).thenComparingLong(SSTMeta::maxFlushId))
            .toList()) {
            if (meta.maxFlushId() < next) {
                continue;
            }
            if (meta.minFlushId() > next) {
                return false;
            }
            next = meta.maxFlushId() + 1;
            if (next > maxFlushId) {
                return true;
            }
        }
        return false;
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

    private void validateNoOverlappingRuns() {
        List<SSTMeta> sorted = metas.values().stream()
            .sorted(Comparator.comparingLong(SSTMeta::minFlushId).thenComparingLong(SSTMeta::maxFlushId))
            .toList();
        for (int i = 1; i < sorted.size(); i++) {
            SSTMeta previous = sorted.get(i - 1);
            SSTMeta current = sorted.get(i);
            if (previous.maxFlushId() >= current.minFlushId()) {
                throw new IllegalArgumentException(
                    "overlapping SST flush ranges: " + previous.path() + " and " + current.path()
                );
            }
        }
    }

    private static boolean overlaps(SSTMeta left, SSTMeta right) {
        return left.minFlushId() <= right.maxFlushId() && right.minFlushId() <= left.maxFlushId();
    }

    private static boolean contains(SSTMeta outer, SSTMeta inner) {
        return outer.minFlushId() <= inner.minFlushId() && outer.maxFlushId() >= inner.maxFlushId();
    }

    private static List<SSTMeta> reconcileVisibleMetas(List<SSTMeta> loaded) {
        List<SSTMeta> sorted = loaded.stream()
            .sorted(Comparator.comparingLong(SSTMeta::minFlushId).thenComparing(Comparator.comparingLong(SSTMeta::maxFlushId).reversed()))
            .toList();
        List<SSTMeta> result = new ArrayList<>();
        candidates:
        for (SSTMeta candidate : sorted) {
            for (int i = 0; i < result.size(); ) {
                SSTMeta existing = result.get(i);
                if (!overlaps(existing, candidate)) {
                    i++;
                    continue;
                }
                if (existing.state() == candidate.state() && contains(existing, candidate)) {
                    continue candidates;
                }
                if (existing.state() == candidate.state() && contains(candidate, existing)) {
                    result.remove(i);
                    continue;
                }
                throw new IllegalArgumentException(
                    "overlapping SST flush ranges: " + existing.path() + " and " + candidate.path()
                );
            }
            result.add(candidate);
        }
        return result.stream()
            .sorted(Comparator.comparingLong(SSTMeta::maxFlushId).thenComparingLong(SSTMeta::minFlushId))
            .toList();
    }

    private static long[] parseFlushRange(Path path) {
        String name = path.getFileName().toString();
        if (!name.startsWith("sst-") || !name.endsWith(".sst")) {
            return new long[] {0, 0};
        }
        String body = name.substring(4, name.length() - 4);
        int dot = body.indexOf('.');
        String range = dot >= 0 ? body.substring(0, dot) : body;
        String[] parts = range.split("-");
        if (parts.length >= 2) {
            return new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        }
        long id = Long.parseLong(range);
        return new long[] {id, id};
    }

    private static SSTState stateFromPath(Path path) {
        String name = path.getFileName().toString();
        return name.contains(".sinked.") ? SSTState.SINKED : SSTState.NEW;
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

    private static void validateMetaMatchesFile(SSTMeta meta, SSTMeta actual) {
        if (meta.fileSize() != actual.fileSize()
            || meta.entryCount() != actual.entryCount()
            || meta.minSequenceId() != actual.minSequenceId()
            || meta.maxSequenceId() != actual.maxSequenceId()
            || meta.createdAtMillis() != actual.createdAtMillis()
            || !java.util.Objects.equals(meta.minKey(), actual.minKey())
            || !java.util.Objects.equals(meta.maxKey(), actual.maxKey())) {
            throw new IllegalArgumentException("SST metadata does not match SST file: " + meta.path());
        }
    }

    private void validateFlushBoundaryCoveredBySST() throws IOException {
        if (lastFlushedSequenceId <= 0) {
            return;
        }
        long maxRecoveredSequenceId = metas.values().stream()
            .mapToLong(SSTMeta::maxSequenceId)
            .max()
            .orElse(0);
        if (maxRecoveredSequenceId < lastFlushedSequenceId) {
            throw new IOException(
                "SST files are missing after flush boundary was persisted: lastFlushedSequenceId="
                    + lastFlushedSequenceId
                    + ", maxRecoveredSequenceId="
                    + maxRecoveredSequenceId
                    + ", storageDir="
                    + dir
            );
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
