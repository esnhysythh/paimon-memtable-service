package org.qwh.pms.core.bucket.operation;

import java.util.Objects;

public record FreezeResult(
    OperationStatus status,
    long fenceSequenceId,
    long frozenMinSequenceId,
    long frozenMaxSequenceId,
    long oldestWriteAtMillis,
    int frozenEntryCount,
    long frozenSizeBytes
) {
    public FreezeResult {
        Objects.requireNonNull(status, "status must not be null");
        if (status == OperationStatus.PROGRESSED) {
            if (frozenEntryCount <= 0 || frozenMinSequenceId <= 0
                    || frozenMaxSequenceId < frozenMinSequenceId
                    || fenceSequenceId < frozenMaxSequenceId
                    || oldestWriteAtMillis <= 0 || frozenSizeBytes <= 0) {
                throw new IllegalArgumentException("invalid progressed freeze result");
            }
        } else if (frozenMinSequenceId != 0 || frozenMaxSequenceId != 0
                || oldestWriteAtMillis != 0 || frozenEntryCount != 0 || frozenSizeBytes != 0) {
            throw new IllegalArgumentException("noop freeze result must not contain a frozen range");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static FreezeResult noop(long fenceSequenceId) {
        return new FreezeResult(OperationStatus.NOOP, fenceSequenceId, 0, 0, 0, 0, 0);
    }
}
