package org.qwh.pms.core.bucket.operation;

import java.util.List;
import java.util.Objects;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.storage.SSTState;

public record CompactionResult(OperationStatus status, List<Group> groups) {
    public CompactionResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(groups, "groups must not be null");
        groups = List.copyOf(groups);
        if (status == OperationStatus.PROGRESSED != !groups.isEmpty()) {
            throw new IllegalArgumentException("compaction progress must match group presence");
        }
    }

    public boolean progressed() {
        return status == OperationStatus.PROGRESSED;
    }

    public static CompactionResult noop() {
        return new CompactionResult(OperationStatus.NOOP, List.of());
    }

    public record Group(SSTState state, List<Long> inputRunIds, LocalRunSnapshot outputRun) {
        public Group {
            Objects.requireNonNull(state, "state must not be null");
            Objects.requireNonNull(inputRunIds, "inputRunIds must not be null");
            Objects.requireNonNull(outputRun, "outputRun must not be null");
            inputRunIds = List.copyOf(inputRunIds);
            if (inputRunIds.size() < 2) {
                throw new IllegalArgumentException("compaction group must contain at least two inputs");
            }
            if (outputRun.state() != state) {
                throw new IllegalArgumentException("compaction output state must match input state");
            }
        }
    }
}
