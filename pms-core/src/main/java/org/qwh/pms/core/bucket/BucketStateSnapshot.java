package org.qwh.pms.core.bucket;

public record BucketStateSnapshot(
    int curMemTableEstimatedEntryCount,
    long curMemTableSizeBytes,
    int immutableMemTableCount,
    long immutableMemTableTotalBytes,
    long lastAssignedSequenceId,
    long curMemTableMinSequenceId,
    long curMemTableMaxSequenceId,
    long immutableMemTableMinSequenceId,
    long immutableMemTableMaxSequenceId,
    long lastFlushedSequenceId,
    int newSSTCount,
    long newSSTTotalBytes,
    long newSSTTotalRows,
    long newSSTMinSequenceId,
    long newSSTMaxSequenceId,
    int sinkedSSTCount,
    long sinkedSSTTotalBytes,
    long sinkedSSTTotalRows,
    int withMemCount,
    long withMemTotalBytes,
    long lastSinkedSnapshotId
) {}
