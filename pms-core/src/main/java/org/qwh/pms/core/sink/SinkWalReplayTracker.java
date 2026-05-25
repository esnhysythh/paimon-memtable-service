package org.qwh.pms.core.sink;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class SinkWalReplayTracker {
    private final Map<String, PreparedSinkCommit> pendingPrepares = new LinkedHashMap<>();
    private final Set<Long> sinkedSSTIds = new HashSet<>();
    private long lastSinkedSnapshotId;

    public void onSinkPrepare(byte[] payload) {
        if (payload.length == 0) {
            return;
        }
        PreparedSinkCommit prepared = SinkWalCodec.decodePrepare(payload);
        pendingPrepares.put(prepared.batchId(), prepared);
    }

    public void onSinkSuccess(long snapshotId, byte[] metadata) {
        lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, snapshotId);
        if (metadata.length == 0) {
            return;
        }
        SinkCommitResult result = SinkWalCodec.decodeSuccess(metadata);
        sinkedSSTIds.addAll(result.sstIds());
        pendingPrepares.remove(result.batchId());
        lastSinkedSnapshotId = Math.max(lastSinkedSnapshotId, result.snapshotId());
    }

    public SinkRecoveryState state() {
        return new SinkRecoveryState(
            sinkedSSTIds,
            pendingPrepares.values().stream().toList(),
            lastSinkedSnapshotId
        );
    }
}
