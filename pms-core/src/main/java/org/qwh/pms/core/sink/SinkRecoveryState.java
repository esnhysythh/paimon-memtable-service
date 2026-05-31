package org.qwh.pms.core.sink;

import java.util.List;
import java.util.Set;

public record SinkRecoveryState(
    Set<Long> sinkedSSTIds,
    List<PreparedSinkCommit> pendingPrepares,
    long lastSinkedSnapshotId,
    long lastPersistedSequenceId
) {
    public SinkRecoveryState {
        sinkedSSTIds = Set.copyOf(sinkedSSTIds);
        pendingPrepares = List.copyOf(pendingPrepares);
    }
}
