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
    long newSSTMinSequenceId,
    long newSSTMaxSequenceId,
    int sinkedSSTCount,
    long sinkedSSTTotalBytes,
    int withMemCount,
    long withMemTotalBytes,
    long lastSinkedSnapshotId
) {}
