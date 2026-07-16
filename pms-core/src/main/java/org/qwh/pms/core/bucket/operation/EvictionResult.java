package org.qwh.pms.core.bucket.operation;

import java.util.Objects;
import java.util.Optional;
import org.qwh.pms.core.bucket.LocalRunSnapshot;

public record EvictionResult(
    OperationStatus status,
    Optional<LocalRunSnapshot> evictedRun
) {
    public EvictionResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(evictedRun, "evictedRun must not be null");
        if (status == OperationStatus.PROGRESSED != evictedRun.isPresent()) {
            throw new IllegalArgumentException("eviction progress must match evicted run presence");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static EvictionResult noop() {
        return new EvictionResult(OperationStatus.NOOP, Optional.empty());
    }
}
