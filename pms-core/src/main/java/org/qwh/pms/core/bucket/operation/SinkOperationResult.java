package org.qwh.pms.core.bucket.operation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.sink.SinkCommitResult;

public record SinkOperationResult(
    OperationStatus status,
    Optional<SinkCommitResult> commitResult,
    List<LocalRunSnapshot> sinkedRuns
) {
    public SinkOperationResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(commitResult, "commitResult must not be null");
        Objects.requireNonNull(sinkedRuns, "sinkedRuns must not be null");
        sinkedRuns = List.copyOf(sinkedRuns);
        if (status == OperationStatus.PROGRESSED != commitResult.isPresent()) {
            throw new IllegalArgumentException("sink progress must match commitResult presence");
        }
        if (status == OperationStatus.NOOP && !sinkedRuns.isEmpty()) {
            throw new IllegalArgumentException("noop sink result must not contain sinked runs");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static SinkOperationResult noop() {
        return new SinkOperationResult(OperationStatus.NOOP, Optional.empty(), List.of());
    }
}
