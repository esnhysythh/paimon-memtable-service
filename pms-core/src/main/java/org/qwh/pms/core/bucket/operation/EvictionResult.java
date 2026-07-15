package org.qwh.pms.core.bucket.operation;

import java.util.Objects;
import java.util.Optional;
import org.qwh.pms.core.bucket.LocalRunSnapshot;

public record EvictionResult(
    OperationStatus status,
    CompactionResult compaction,
    Optional<LocalRunSnapshot> evictedRun
) {
    public EvictionResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(compaction, "compaction must not be null");
        Objects.requireNonNull(evictedRun, "evictedRun must not be null");
        boolean hasProgress = compaction.progressed() || evictedRun.isPresent();
        if (status == OperationStatus.PROGRESSED != hasProgress) {
            throw new IllegalArgumentException("eviction progress must match compact/evict output");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static EvictionResult noop() {
        return new EvictionResult(OperationStatus.NOOP, CompactionResult.noop(), Optional.empty());
    }
}
