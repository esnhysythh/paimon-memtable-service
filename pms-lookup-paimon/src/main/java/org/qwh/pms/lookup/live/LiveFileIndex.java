package org.qwh.pms.lookup.live;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.Levels;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Comparator;
import java.util.Set;

/**
 * Maintains PMS-owned live Paimon data-file views.
 */
public class LiveFileIndex {

    private final Comparator<InternalRow> keyComparator;
    private final int numLevels;
    private final Map<PartitionBucket, BucketState> buckets = new HashMap<>();

    public LiveFileIndex(Comparator<InternalRow> keyComparator, int numLevels) {
        this.keyComparator = keyComparator;
        this.numLevels = numLevels;
    }

    /** Installs a complete committed file snapshot for one partition-bucket. */
    public synchronized void installSnapshot(
            BinaryRow partition, int bucket, List<DataFileMeta> liveFiles) {
        PartitionBucket key = PartitionBucket.of(partition, bucket);
        List<DataFileMeta> files = liveFiles == null ? Collections.emptyList() : List.copyOf(liveFiles);
        BucketState previous = buckets.get(key);
        buckets.put(
                key,
                BucketState.valid(
                        new Levels(keyComparator, files, restoredNumLevels(files)),
                        nextVersion(previous)));
    }

    public synchronized void applyDelta(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> afterFiles) {
        PartitionBucket key = PartitionBucket.of(partition, bucket);
        List<DataFileMeta> before = beforeFiles == null ? Collections.emptyList() : beforeFiles;
        List<DataFileMeta> after = afterFiles == null ? Collections.emptyList() : afterFiles;
        BucketState state = buckets.get(key);
        if (state == null) {
            throw new IllegalStateException("Cannot apply delta without an installed snapshot: " + key);
        }
        if (!state.valid) {
            throw new IllegalStateException("Cannot apply delta to an invalid bucket: " + key);
        }

        try {
            List<DataFileMeta> currentFiles = state.levels.allFiles();
            Levels updated =
                    new Levels(
                            keyComparator,
                            currentFiles,
                            updatedNumLevels(state.levels, after));
            ensureBeforeFilesAreLive(updated, before);
            updated.update(before, after);
            buckets.put(key, BucketState.valid(updated, nextVersion(state)));
        } catch (RuntimeException e) {
            buckets.put(key, BucketState.invalid(nextVersion(state)));
            throw e;
        }
    }

    public synchronized Optional<LiveBucketView> bucketView(BinaryRow partition, int bucket) {
        BucketState state = buckets.get(PartitionBucket.of(partition, bucket));
        if (state == null) {
            return Optional.empty();
        }
        return Optional.of(new LiveBucketView(state.levels, state.valid, state.version));
    }

    public synchronized void invalidate(BinaryRow partition, int bucket) {
        PartitionBucket key = PartitionBucket.of(partition, bucket);
        buckets.put(key, BucketState.invalid(nextVersion(buckets.get(key))));
    }

    private int restoredNumLevels(List<DataFileMeta> files) {
        int highestFileLevel = files.stream().mapToInt(DataFileMeta::level).max().orElse(-1);
        return Math.max(numLevels, highestFileLevel + 1);
    }

    private int updatedNumLevels(Levels currentLevels, List<DataFileMeta> afterFiles) {
        return Math.max(currentLevels.numberOfLevels(), restoredNumLevels(afterFiles));
    }

    private long nextVersion(BucketState state) {
        return state == null ? 1L : state.version + 1L;
    }

    private void ensureBeforeFilesAreLive(Levels levels, List<DataFileMeta> beforeFiles) {
        Set<String> liveNames = new HashSet<>();
        for (DataFileMeta file : levels.allFiles()) {
            liveNames.add(file.fileName());
        }
        for (DataFileMeta file : beforeFiles) {
            if (!liveNames.contains(file.fileName())) {
                throw new IllegalStateException("Delta removes non-live file: " + file.fileName());
            }
        }
    }

    private static final class BucketState {

        private final Levels levels;
        private final boolean valid;
        private final long version;

        private BucketState(Levels levels, boolean valid, long version) {
            this.levels = levels;
            this.valid = valid;
            this.version = version;
        }

        private static BucketState valid(Levels levels, long version) {
            return new BucketState(levels, true, version);
        }

        private static BucketState invalid(long version) {
            return new BucketState(null, false, version);
        }
    }
}
