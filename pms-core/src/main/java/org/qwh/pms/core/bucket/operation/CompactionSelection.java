package org.qwh.pms.core.bucket.operation;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.qwh.pms.core.storage.SSTState;

/** Exact same-state, continuous local runs selected for one compaction operation. */
public record CompactionSelection(SSTState state, List<Long> inputRunIds) {
    public CompactionSelection {
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(inputRunIds, "inputRunIds must not be null");
        inputRunIds = List.copyOf(inputRunIds);
        if (inputRunIds.size() < 2) {
            throw new IllegalArgumentException("compaction selection must contain at least two runs");
        }
        if (inputRunIds.stream().anyMatch(runId -> runId == null || runId <= 0)) {
            throw new IllegalArgumentException("compaction run ids must be positive");
        }
        if (new HashSet<>(inputRunIds).size() != inputRunIds.size()) {
            throw new IllegalArgumentException("compaction run ids must be unique");
        }
    }
}
