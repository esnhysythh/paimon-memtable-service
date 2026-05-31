package org.qwh.pms.core.storage;

import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class FileLocalStorageManager implements LocalStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileLocalStorageManager.class);

    private final Path dir;
    private final FlushBoundaryStore flushBoundaryStore;
    private final SSTMetaStore sstMetaStore;
    private final TreeMap<Long, SSTMeta> metas = new TreeMap<>();
    private final Map<Long, SSTReader> readers = new HashMap<>();
    private final AtomicLong nextFileId = new AtomicLong(1);
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
        long maxFileId = 0;
        for (Path path : sstMetaStore.listMetaFiles()) {
            maxFileId = Math.max(maxFileId, SSTMetaStore.fileIdFromMetaPath(path));
            try {
                SSTMeta meta = sstMetaStore.load(path);
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
                metas.put(meta.fileId(), meta);
                readers.put(meta.fileId(), SSTReader.open(meta));
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
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sst-*.sst")) {
            for (Path path : stream) {
                long fileId = parseFileId(path);
                maxFileId = Math.max(maxFileId, fileId);
                if (metas.containsKey(fileId) || Files.exists(sstMetaStore.metaPath(fileId))) {
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
        nextFileId.set(maxFileId + 1);
    }

    public synchronized List<SSTMeta> metas() {
        return List.copyOf(metas.values());
    }

    public synchronized List<SSTMeta> metas(SSTState state) {
        return metas.values().stream()
            .filter(meta -> meta.state() == state)
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

    public synchronized void applySinkedSSTIds(Set<Long> sinkedIds) {
        for (SSTMeta meta : List.copyOf(metas.values())) {
            SSTState target = sinkedIds.contains(meta.fileId()) ? SSTState.SINKED : SSTState.NEW;
            if (meta.state() != target || !meta.path().equals(pathFor(meta.fileId(), target))) {
                updateStateLabel(meta, target);
            }
        }
    }

    public synchronized List<SSTMeta> markSinked(List<SSTMeta> toMark) {
        for (SSTMeta meta : toMark) {
            SSTMeta current = metas.get(meta.fileId());
            if (current != null) {
                updateStateLabel(current, SSTState.SINKED);
            }
        }
        return metas(SSTState.SINKED);
    }

    @Override
    public synchronized SSTMeta flushToSST(ImmutableMemTable memTable) {
        try {
            SSTMeta meta = new SSTWriter(
                dir,
                SSTFormat.DEFAULT_BLOCK_SIZE,
                SSTFormat.DEFAULT_RESTART_INTERVAL
            ).write(nextFileId.getAndIncrement(), memTable);
            sstMetaStore.save(meta);
            metas.put(meta.fileId(), meta);
            readers.put(meta.fileId(), SSTReader.open(meta));
            return meta;
        } catch (IOException e) {
            throw new RuntimeException("flush to SST failed", e);
        }
    }

    @Override
    public synchronized Optional<Value> get(SSTMeta meta, Key key) {
        try {
            return readerFor(meta).get(key);
        } catch (IOException e) {
            throw new RuntimeException("SST read failed: " + meta.path(), e);
        }
    }

    @Override
    public synchronized SSTEntryIterator openIterator(SSTMeta meta) {
        try {
            return readerFor(meta).iterator();
        } catch (IOException e) {
            throw new RuntimeException("SST iterator open failed: " + meta.path(), e);
        }
    }

    @Override
    public synchronized SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
        try {
            return readerFor(meta).iterator(startInclusive, endExclusive);
        } catch (IOException e) {
            throw new RuntimeException("SST range iterator open failed: " + meta.path(), e);
        }
    }

    @Override
    public SSTMeta compactSSTs(List<SSTMeta> metas) {
        throw new UnsupportedOperationException("SST compaction is not implemented yet");
    }

    @Override
    public synchronized void deleteSST(SSTMeta meta) {
        try {
            Files.deleteIfExists(meta.path());
            Files.deleteIfExists(sstMetaStore.metaPath(meta.fileId()));
            metas.remove(meta.fileId());
            readers.remove(meta.fileId());
        } catch (IOException e) {
            throw new RuntimeException("delete SST failed: " + meta.path(), e);
        }
    }

    @Override
    public synchronized Optional<SSTMeta> evictOldestSinkedSST() {
        Optional<SSTMeta> oldest = metas.values().stream()
            .filter(meta -> meta.state() == SSTState.SINKED)
            .min(Comparator.comparingLong(SSTMeta::createdAtMillis).thenComparingLong(SSTMeta::fileId));
        oldest.ifPresent(this::deleteSST);
        return oldest;
    }

    private void updateStateLabel(SSTMeta meta, SSTState target) {
        Path targetPath = pathFor(meta.fileId(), target);
        Path newPath = meta.path();
        if (!meta.path().equals(targetPath)) {
            try {
                try {
                    Files.move(meta.path(), targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(meta.path(), targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
                newPath = targetPath;
            } catch (IOException e) {
                LOG.warn("Failed to rename SST state label from {} to {}", meta.path(), targetPath, e);
            }
        }
        SSTMeta updated = meta.withPathAndState(newPath, target);
        try {
            sstMetaStore.save(updated);
        } catch (IOException e) {
            throw new RuntimeException("persist SST metadata failed: " + sstMetaStore.metaPath(meta.fileId()), e);
        }
        metas.put(meta.fileId(), updated);
        readers.remove(meta.fileId());
    }

    private SSTReader readerFor(SSTMeta meta) throws IOException {
        SSTReader reader = readers.get(meta.fileId());
        if (reader != null && reader.meta().path().equals(meta.path())) {
            return reader;
        }
        reader = SSTReader.open(meta);
        readers.put(meta.fileId(), reader);
        return reader;
    }

    private Path pathFor(long fileId, SSTState state) {
        String label = state == SSTState.SINKED ? "sinked" : "new";
        return dir.resolve(String.format("sst-%06d.%s.sst", fileId, label));
    }

    private static long parseFileId(Path path) {
        String name = path.getFileName().toString();
        if (!name.startsWith("sst-") || !name.endsWith(".sst")) {
            return 0;
        }
        String body = name.substring(4, name.length() - 4);
        int dot = body.indexOf('.');
        String id = dot >= 0 ? body.substring(0, dot) : body;
        return Long.parseLong(id);
    }

    private static void validateMetaMatchesFile(SSTMeta meta, SSTMeta actual) {
        if (meta.fileId() != actual.fileId()
            || meta.fileSize() != actual.fileSize()
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
}
