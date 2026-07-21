package org.qwh.pms.core.bucket;

import java.util.List;
import java.util.Objects;

public record BucketStateSnapshot(
    long observedAtMillis,
    int curMemTableEstimatedEntryCount,
    long curMemTableSizeBytes,
    long curMemTableMinSequenceId,
    long curMemTableMaxSequenceId,
    long curMemTableOldestWriteAtMillis,
    int immutableMemTableCount,
    long immutableMemTableTotalBytes,
    long immutableMemTableMinSequenceId,
    long immutableMemTableMaxSequenceId,
    long immutableMemTableOldestWriteAtMillis,
    long lastAssignedSequenceId,
    long lastFlushedSequenceId,
    long lastPersistedSequenceId,
    int newSSTCount,
    long newSSTTotalBytes,
    long newSSTTotalRows,
    long newSSTMinSequenceId,
    long newSSTMaxSequenceId,
    long newSSTOldestWriteAtMillis,
    int sinkedSSTCount,
    long sinkedSSTTotalBytes,
    long sinkedSSTTotalRows,
    long sinkedSSTMinSequenceId,
    long sinkedSSTMaxSequenceId,
    long sinkedSSTOldestWriteAtMillis,
    List<LocalRunSnapshot> localRuns,
    SinkFlightSnapshot sinkFlight,
    boolean recoveredUnpersistedData,
    long lastSinkedSnapshotId
) {
    public BucketStateSnapshot {
        Objects.requireNonNull(localRuns, "localRuns must not be null");
        Objects.requireNonNull(sinkFlight, "sinkFlight must not be null");
        localRuns = List.copyOf(localRuns);
    }
}
