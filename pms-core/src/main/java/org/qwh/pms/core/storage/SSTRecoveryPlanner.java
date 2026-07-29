package org.qwh.pms.core.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Rebuilds the visible SST suffix and identifies files left by interrupted maintenance.
 *
 * <p>V1 only recovers normal compact, oldest-evict and unpublished-Flush residues. Other
 * incomplete or conflicting ranges are treated as corruption.
 */
final class SSTRecoveryPlanner {
    private final Path dir;
    private final SSTMetaStore metaStore;

    SSTRecoveryPlanner(Path dir, SSTMetaStore metaStore) {
        this.dir = Objects.requireNonNull(dir, "dir must not be null");
        this.metaStore = Objects.requireNonNull(metaStore, "metaStore must not be null");
    }

    RecoveryPlan recover(long lastFlushedSequenceId, long lastPersistedSequenceId)
            throws IOException {
        if (lastPersistedSequenceId > lastFlushedSequenceId) {
            throw new IOException(
                "Sink boundary is ahead of the local flush boundary: lastPersistedSequenceId="
                    + lastPersistedSequenceId
                    + ", lastFlushedSequenceId="
                    + lastFlushedSequenceId
            );
        }

        List<SSTMeta> complete = new ArrayList<>();
        List<SSTMeta> missingData = new ArrayList<>();
        List<SSTMeta> orphans = new ArrayList<>();
        Set<Path> dataFilesWithMeta = new HashSet<>();
        long maxRunId = 0;

        // Metadata gives us runId and sequence boundaries. Only complete pairs participate in
        // the visible-run selection; missing data is classified after that selection is known.
        for (Path metaPath : metaStore.listMetaFiles()) {
            SSTMeta meta;
            try {
                meta = metaStore.load(metaPath);
            } catch (IOException | RuntimeException e) {
                throw new IOException("Cannot load SST metadata: " + metaPath, e);
            }
            maxRunId = Math.max(maxRunId, meta.runId());
            dataFilesWithMeta.add(meta.path());

            long[] fileRange = SSTMetaStore.flushRangeFromMetaPath(metaPath);
            if (fileRange[0] != meta.minFlushId() || fileRange[1] != meta.maxFlushId()) {
                throw new IOException("SST metadata range does not match its file name: " + metaPath);
            }

            // Boundary orphans were never handed off from WAL to SST. Leave them in place so the
            // next Flush can overwrite the same logical flushId.
            if (meta.minSequenceId() > lastFlushedSequenceId) {
                orphans.add(meta);
                continue;
            }
            if (meta.maxSequenceId() > lastFlushedSequenceId) {
                throw new IOException(
                    "SST crosses lastFlushedSequenceId: " + meta.path()
                );
            }
            if (!Files.exists(meta.path())) {
                missingData.add(meta);
                continue;
            }

            SSTMeta actual;
            try {
                actual = SSTReader.readMeta(meta.path(), meta.state());
            } catch (IOException | RuntimeException e) {
                throw new IOException("Cannot read SST data: " + meta.path(), e);
            }
            if (meta.minFlushId() != actual.minFlushId()
                    || meta.maxFlushId() != actual.maxFlushId()
                    || meta.fileSize() != actual.fileSize()
                    || meta.entryCount() != actual.entryCount()
                    || meta.minSequenceId() != actual.minSequenceId()
                    || meta.maxSequenceId() != actual.maxSequenceId()
                    || meta.oldestWriteAtMillis() != actual.oldestWriteAtMillis()
                    || meta.createdAtMillis() != actual.createdAtMillis()
                    || !Objects.equals(meta.minKey(), actual.minKey())
                    || !Objects.equals(meta.maxKey(), actual.maxKey())) {
                throw new IOException("SST metadata does not match SST data: " + meta.path());
            }

            complete.add(meta);
        }

        complete.sort(
            Comparator.comparingLong(SSTMeta::minFlushId)
                .thenComparingLong(SSTMeta::maxFlushId)
        );
        // One candidate is enough: keep the widest range at each start, discard ranges fully
        // covered by it, and publish it only when the next adjacent range is reached.
        List<SSTMeta> visible = new ArrayList<>();
        List<SSTMeta> cleanupMetas = new ArrayList<>();
        SSTMeta candidate = null;
        for (SSTMeta current : complete) {
            if (candidate == null) {
                // The oldest complete SST starts the first candidate range.
                candidate = current;
                continue;
            }
            if (candidate.minFlushId() == current.minFlushId()) {
                // A compact output and its first input share the same start; keep the wider one.
                if (candidate.maxFlushId() >= current.maxFlushId()) {
                    cleanupMetas.add(current);
                } else {
                    cleanupMetas.add(candidate);
                    candidate = current;
                }
                continue;
            }
            if (current.minFlushId() <= candidate.maxFlushId()) {
                if (current.maxFlushId() <= candidate.maxFlushId()) {
                    // The candidate is a compact output covering this later old input.
                    cleanupMetas.add(current);
                    continue;
                }
                // Neither range contains the other, so this cannot be a valid compact residue.
                throw new IOException(
                    "SST flush ranges partially overlap: "
                        + candidate.path()
                        + " and "
                        + current.path()
                );
            }
            if (candidate.maxFlushId() + 1 != current.minFlushId()) {
                // The ranges are disjoint but some flushId is not represented by any SST.
                throw new IOException(
                    "Recovered SST flush IDs are not continuous: previous=["
                        + candidate.minFlushId()
                        + ","
                        + candidate.maxFlushId()
                        + "], current=["
                        + current.minFlushId()
                        + ","
                        + current.maxFlushId()
                        + "]"
                );
            }
            // Exact adjacency confirms the candidate; current becomes the next candidate.
            visible.add(candidate);
            candidate = current;
        }
        if (candidate != null) {
            visible.add(candidate);
        }

        for (int i = 0; i < visible.size(); i++) {
            SSTMeta meta = visible.get(i);
            SSTState state;
            if (meta.maxSequenceId() <= lastPersistedSequenceId) {
                state = SSTState.SINKED;
            } else if (meta.minSequenceId() > lastPersistedSequenceId) {
                state = SSTState.NEW;
            } else {
                throw new IOException(
                    "SST crosses lastPersistedSequenceId: " + meta.path()
                );
            }
            if (i > 0
                    && visible.get(i - 1).state() == SSTState.NEW
                    && state == SSTState.SINKED) {
                throw new IOException("Recovered SST states are out of order: " + meta.path());
            }
            visible.set(i, meta.withPathAndState(meta.path(), state));
        }

        if (lastFlushedSequenceId > lastPersistedSequenceId
                && (visible.isEmpty()
                    || visible.get(visible.size() - 1).maxSequenceId()
                        != lastFlushedSequenceId)) {
            throw new IOException(
                "Unpersisted flush boundary is not covered by recovered SSTs: "
                    + "lastFlushedSequenceId="
                    + lastFlushedSequenceId
                    + ", lastPersistedSequenceId="
                    + lastPersistedSequenceId
            );
        }

        // A missing data file is recoverable only after compact made it redundant, or when
        // data-first eviction had already started on the oldest Paimon-persisted prefix.
        for (SSTMeta meta : missingData) {
            boolean coveredByCompact = !visible.isEmpty()
                && meta.minFlushId() >= visible.get(0).minFlushId()
                && meta.maxFlushId()
                    <= visible.get(visible.size() - 1).maxFlushId();
            boolean oldestEvictResidue = meta.maxSequenceId() <= lastPersistedSequenceId
                && (visible.isEmpty()
                    || meta.maxFlushId() < visible.get(0).minFlushId());
            if (!coveredByCompact && !oldestEvictResidue) {
                throw new IOException(
                    "SST data is missing inside the retained suffix: " + meta.path()
                );
            }
            cleanupMetas.add(meta);
        }

        long nextFlushId = visible.isEmpty()
            ? 1
            : visible.get(visible.size() - 1).maxFlushId() + 1;
        for (SSTMeta orphan : orphans) {
            if (orphan.minFlushId() != nextFlushId
                    || orphan.maxFlushId() != nextFlushId) {
                throw new IOException(
                    "Boundary orphan does not match the next flushId: " + orphan.path()
                );
            }
        }
        int orphanCount = orphans.size();
        List<Path> cleanupDataFiles = new ArrayList<>();
        // Data without metadata is either the next unpublished Flush or an incomplete compact
        // output already covered by the selected suffix.
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sst-*.sst")) {
            for (Path dataPath : stream) {
                if (dataFilesWithMeta.contains(dataPath)) {
                    continue;
                }
                String name = dataPath.getFileName().toString();
                String[] range = name.substring(4, name.length() - 4).split("-");
                if (range.length != 2) {
                    throw new IOException("Invalid SST data file name: " + dataPath);
                }
                long minFlushId;
                long maxFlushId;
                try {
                    minFlushId = Long.parseLong(range[0]);
                    maxFlushId = Long.parseLong(range[1]);
                } catch (RuntimeException e) {
                    throw new IOException("Invalid SST data file name: " + dataPath, e);
                }
                if (minFlushId == nextFlushId && maxFlushId == nextFlushId) {
                    orphanCount++;
                    continue;
                }
                boolean covered = !visible.isEmpty()
                    && minFlushId >= visible.get(0).minFlushId()
                    && maxFlushId
                        <= visible.get(visible.size() - 1).maxFlushId();
                if (!covered) {
                    throw new IOException(
                        "SST metadata is missing outside the retained suffix: " + dataPath
                    );
                }
                cleanupDataFiles.add(dataPath);
            }
        }
        if (orphanCount > 1) {
            throw new IOException("More than one boundary orphan uses flushId " + nextFlushId);
        }

        return new RecoveryPlan(
            visible,
            cleanupMetas,
            cleanupDataFiles,
            maxRunId + 1,
            nextFlushId
        );
    }

    record RecoveryPlan(
        List<SSTMeta> visibleMetas,
        List<SSTMeta> cleanupMetas,
        List<Path> cleanupDataFiles,
        long nextRunId,
        long nextFlushId
    ) {
        RecoveryPlan {
            visibleMetas = List.copyOf(visibleMetas);
            cleanupMetas = List.copyOf(cleanupMetas);
            cleanupDataFiles = List.copyOf(cleanupDataFiles);
        }
    }
}
