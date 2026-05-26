package org.qwh.pms.core.bucket;

public record RecoverySummary(
    long recoveredDataRecords,
    long skippedFlushedRecords,
    long lastFlushedSequenceId,
    long pendingPreparedSinkCount,
    long recoveredPreparedSinkCount,
    long recoveredSinkedSSTCount,
    long lastSinkedSnapshotId,
    int newSSTCount,
    int sinkedSSTCount,
    int curMemTableEstimatedEntryCount
) {
    public static RecoverySummary empty() {
        return new RecoverySummary(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
