package org.qwh.pms.core.bucket;

public record BucketStateSnapshot(
    int curMemTableEntryCount,
    long curMemTableSizeBytes,
    int immutableMemTableCount,
    long immutableMemTableTotalBytes,
    long lastSinkedSnapshotId
) {}
