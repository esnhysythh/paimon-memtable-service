package org.qwh.pms.core.storage;

import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
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
    private final ConcurrentHashMap<Long, ReaderHolder> readers = new ConcurrentHashMap<>();
    private final AtomicLong nextRunId = new AtomicLong(1);
    private final AtomicLong nextFlushId = new AtomicLong(1);
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
            readers.put(meta.runId(), new ReaderHolder(SSTReader.open(meta)));
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
        for (ReaderHolder holder : readers.values()) {
            holder.retire(null);
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
            if (meta.state() != target || !meta.path().equals(pathFor(meta.minFlushId(), meta.maxFlushId(), target))) {
                updateStateLabel(meta, target);
            }
        }
    }

    public synchronized List<SSTMeta> markSinked(List<SSTMeta> toMark) {
        for (SSTMeta meta : toMark) {
            SSTMeta current = metas.get(meta.runId());
            if (current != null) {
                updateStateLabel(current, SSTState.SINKED);
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
            readers.put(meta.runId(), new ReaderHolder(SSTReader.open(meta)));
            return meta;
        } catch (IOException e) {
            throw new RuntimeException("flush to SST failed", e);
        }
    }

    @Override
    public Optional<Value> get(SSTMeta meta, Key key) {
        try (ReaderLease lease = acquireReader(meta)) {
            return lease.reader().get(key);
        } catch (IOException e) {
            throw new RuntimeException("SST read failed: " + meta.path(), e);
        }
    }

    @Override
    public SSTEntryIterator openIterator(SSTMeta meta) {
        try {
            ReaderLease lease = acquireReader(meta);
            return new LeasedSSTEntryIterator(lease.reader().iterator(), lease);
        } catch (IOException e) {
            throw new RuntimeException("SST iterator open failed: " + meta.path(), e);
        }
    }

    @Override
    public SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
        try {
            ReaderLease lease = acquireReader(meta);
            return new LeasedSSTEntryIterator(lease.reader().iterator(startInclusive, endExclusive), lease);
        } catch (IOException e) {
            throw new RuntimeException("SST range iterator open failed: " + meta.path(), e);
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
            for (SSTMeta meta : inputs) {
                iterators.add(openIterator(meta));
            }
            long expectedEntries = inputs.stream().mapToLong(SSTMeta::entryCount).sum();
            long minSequenceId = inputs.stream().mapToLong(SSTMeta::minSequenceId).min().orElse(0);
            long maxSequenceId = inputs.stream().mapToLong(SSTMeta::maxSequenceId).max().orElse(0);
            SSTMeta output;
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
            synchronized (this) {
                sstMetaStore.save(output);
                for (SSTMeta input : inputs) {
                    this.metas.remove(input.runId());
                    retireReader(input.runId(), () -> deleteSSTDataFileQuietly(input));
                }
                putVisibleMeta(output);
                readers.put(output.runId(), new ReaderHolder(SSTReader.open(output)));
                for (SSTMeta input : inputs) {
                    deleteSSTMetadata(input);
                }
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
        retireReader(meta.runId(), () -> deleteSSTFiles(meta));
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

    private void deleteSSTDataFileQuietly(SSTMeta meta) {
        try {
            deleteSSTDataFile(meta);
        } catch (IOException e) {
            LOG.warn("Failed to delete obsolete SST data file: {}", meta.path(), e);
        }
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

    private void updateStateLabel(SSTMeta meta, SSTState target) {
        Path targetPath = pathFor(meta.minFlushId(), meta.maxFlushId(), target);
        Path newPath = meta.path();
        try {
            if (!meta.path().equals(targetPath)) {
                try {
                    Files.move(meta.path(), targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(meta.path(), targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
                newPath = targetPath;
            }
        } catch (IOException e) {
            LOG.warn("Failed to rename SST state label from {} to {}", meta.path(), targetPath, e);
        }
        SSTMeta updated = meta.withPathAndState(newPath, target);
        try {
            sstMetaStore.save(updated);
            putVisibleMeta(updated);
            retireReader(meta.runId(), null);
        } catch (IOException e) {
            throw new RuntimeException("persist SST metadata failed: " + sstMetaStore.metaPath(meta), e);
        }
    }

    private ReaderLease acquireReader(SSTMeta meta) throws IOException {
        while (true) {
            SSTMeta effectiveMeta = metas.getOrDefault(meta.runId(), meta);
            ReaderHolder holder = readers.get(effectiveMeta.runId());
            if (holder != null && holder.matches(effectiveMeta)) {
                ReaderLease lease = holder.acquire();
                if (lease != null) {
                    return lease;
                }
            }
            synchronized (this) {
                effectiveMeta = metas.getOrDefault(meta.runId(), meta);
                holder = readers.get(effectiveMeta.runId());
                if (holder != null && holder.matches(effectiveMeta)) {
                    ReaderLease lease = holder.acquire();
                    if (lease != null) {
                        return lease;
                    }
                }
                holder = new ReaderHolder(SSTReader.open(effectiveMeta));
                readers.put(effectiveMeta.runId(), holder);
                ReaderLease lease = holder.acquire();
                if (lease == null) {
                    throw new IOException("SST reader retired before acquire: " + effectiveMeta.path());
                }
                return lease;
            }
        }
    }

    private void retireReader(long runId, Runnable cleanup) {
        ReaderHolder holder = readers.remove(runId);
        if (holder != null) {
            holder.retire(cleanup);
        } else if (cleanup != null) {
            cleanup.run();
        }
    }

    private static final class ReaderHolder {
        private final SSTReader reader;
        private int leases;
        private boolean retired;
        private Runnable cleanup;

        private ReaderHolder(SSTReader reader) {
            this.reader = reader;
        }

        private boolean matches(SSTMeta meta) {
            return reader.meta().path().equals(meta.path());
        }

        private synchronized ReaderLease acquire() {
            if (retired) {
                return null;
            }
            leases++;
            return new ReaderLease(this, reader);
        }

        private void retire(Runnable cleanup) {
            Runnable toRun;
            synchronized (this) {
                if (retired) {
                    return;
                }
                retired = true;
                this.cleanup = cleanup;
                toRun = cleanupIfIdleLocked();
            }
            runCleanup(toRun);
        }

        private void release() {
            Runnable toRun;
            synchronized (this) {
                if (leases <= 0) {
                    throw new IllegalStateException("SST reader lease released more than once: " + reader.meta().path());
                }
                leases--;
                toRun = cleanupIfIdleLocked();
            }
            runCleanup(toRun);
        }

        private Runnable cleanupIfIdleLocked() {
            if (!retired || leases > 0) {
                return null;
            }
            Runnable toRun = cleanup;
            cleanup = null;
            return () -> {
                try {
                    reader.close();
                } finally {
                    if (toRun != null) {
                        toRun.run();
                    }
                }
            };
        }

        private void runCleanup(Runnable cleanup) {
            if (cleanup == null) {
                return;
            }
            try {
                cleanup.run();
            } catch (RuntimeException e) {
                LOG.warn("SST reader retirement cleanup failed: {}", reader.meta().path(), e);
            }
        }
    }

    private static final class ReaderLease implements AutoCloseable {
        private final ReaderHolder holder;
        private final SSTReader reader;
        private boolean closed;

        private ReaderLease(ReaderHolder holder, SSTReader reader) {
            this.holder = holder;
            this.reader = reader;
        }

        private SSTReader reader() {
            return reader;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            holder.release();
        }
    }

    private static final class LeasedSSTEntryIterator implements SSTEntryIterator {
        private final SSTEntryIterator delegate;
        private final ReaderLease lease;
        private boolean closed;

        private LeasedSSTEntryIterator(SSTEntryIterator delegate, ReaderLease lease) {
            this.delegate = delegate;
            this.lease = lease;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            boolean hasNext = delegate.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return delegate.next();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            try {
                delegate.close();
            } catch (RuntimeException e) {
                failure = e;
            }
            try {
                lease.close();
            } catch (RuntimeException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
                } else {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private Path pathFor(long minFlushId, long maxFlushId, SSTState state) {
        return SSTWriter.pathFor(dir, minFlushId, maxFlushId, state);
    }

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
