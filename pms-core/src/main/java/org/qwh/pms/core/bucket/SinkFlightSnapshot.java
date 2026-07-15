package org.qwh.pms.core.bucket;

import java.util.Objects;

/**
 * Runtime state for the single V1 Sink flight. The flush fence is the logical boundary used by
 * NEW-run compaction; durable Paimon prepare payloads remain owned by SinkMetaStore.
 */
public record SinkFlightSnapshot(
    Status status,
    String batchId,
    long sinkFenceFlushId,
    long minSequenceId,
    long maxSequenceId
) {
    public SinkFlightSnapshot {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(batchId, "batchId must not be null");
        if (status == Status.IDLE) {
            if (!batchId.isEmpty() || sinkFenceFlushId != 0 || minSequenceId != 0 || maxSequenceId != 0) {
                throw new IllegalArgumentException("idle Sink flight must have empty boundaries");
            }
        } else if (batchId.isBlank()
                || sinkFenceFlushId <= 0
                || minSequenceId <= 0
                || maxSequenceId < minSequenceId) {
            throw new IllegalArgumentException("active Sink flight has invalid boundaries");
        }
    }

    public static SinkFlightSnapshot idle() {
        return new SinkFlightSnapshot(Status.IDLE, "", 0, 0, 0);
    }

    public boolean active() {
        return status != Status.IDLE;
    }

    public enum Status {
        IDLE,
        IN_FLIGHT,
        PREPARED_RETRY
    }
}
