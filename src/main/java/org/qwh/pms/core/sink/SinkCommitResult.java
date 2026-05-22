package org.qwh.pms.core.sink;

import java.util.List;

public record SinkCommitResult(
    String batchId,
    long snapshotId,
    long persistedSequenceId,
    List<Long> sstIds
) {
    public SinkCommitResult {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        if (snapshotId <= 0) {
            throw new IllegalArgumentException("snapshotId must be positive");
        }
        if (persistedSequenceId <= 0) {
            throw new IllegalArgumentException("persistedSequenceId must be positive");
        }
        sstIds = List.copyOf(sstIds);
        if (sstIds.isEmpty()) {
            throw new IllegalArgumentException("sstIds must not be empty");
        }
    }
}
