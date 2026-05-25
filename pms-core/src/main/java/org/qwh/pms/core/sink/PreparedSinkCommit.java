package org.qwh.pms.core.sink;

import java.util.List;

public record PreparedSinkCommit(
    String batchId,
    long commitIdentifier,
    List<Long> sstIds,
    long minSequenceId,
    long maxSequenceId,
    byte[] payload,
    List<SinkFileRef> fileRefs,
    long inputRecordCount,
    long outputRecordCount
) {
    public PreparedSinkCommit {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        if (commitIdentifier <= 0) {
            throw new IllegalArgumentException("commitIdentifier must be positive");
        }
        sstIds = List.copyOf(sstIds);
        if (sstIds.isEmpty()) {
            throw new IllegalArgumentException("sstIds must not be empty");
        }
        if (minSequenceId <= 0 || maxSequenceId <= 0 || minSequenceId > maxSequenceId) {
            throw new IllegalArgumentException("invalid sequence bounds");
        }
        payload = payload == null ? new byte[0] : payload.clone();
        fileRefs = List.copyOf(fileRefs);
        if (inputRecordCount < 0 || outputRecordCount < 0) {
            throw new IllegalArgumentException("record counts must be non-negative");
        }
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
