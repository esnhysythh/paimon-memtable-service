package org.qwh.pms.core.bucket.operation;

import java.util.Objects;
import java.util.Optional;
import org.qwh.pms.core.bucket.LocalRunSnapshot;

public record FlushResult(OperationStatus status, Optional<LocalRunSnapshot> outputRun) {
    public FlushResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(outputRun, "outputRun must not be null");
        if (status == OperationStatus.PROGRESSED != outputRun.isPresent()) {
            throw new IllegalArgumentException("flush progress must match outputRun presence");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static FlushResult noop() {
        return new FlushResult(OperationStatus.NOOP, Optional.empty());
    }
}
