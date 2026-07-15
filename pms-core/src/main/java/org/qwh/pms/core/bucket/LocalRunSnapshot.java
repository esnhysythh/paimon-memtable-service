package org.qwh.pms.core.bucket;

import java.util.Objects;
import org.qwh.pms.core.storage.SSTState;

public record LocalRunSnapshot(
    long runId,
    long minFlushId,
    long maxFlushId,
    SSTState state,
    long fileSizeBytes,
    long entryCount,
    long minSequenceId,
    long maxSequenceId,
    long oldestWriteAtMillis,
    long ageMillis
) {
    public LocalRunSnapshot {
        Objects.requireNonNull(state, "state must not be null");
    }
}
