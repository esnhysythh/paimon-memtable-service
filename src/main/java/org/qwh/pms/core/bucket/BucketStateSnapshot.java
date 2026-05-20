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
    // TODO: 待 SST 模块实现后补充以下字段
    // int newSSTCount,
    // long newSSTTotalBytes,
    // int sinkedSSTCount,
    // long sinkedSSTTotalBytes,
    // int withMemCount,
    // long withMemTotalBytes,
    long lastSinkedSnapshotId
) {}
