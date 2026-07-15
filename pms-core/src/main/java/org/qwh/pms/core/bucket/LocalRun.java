package org.qwh.pms.core.bucket;

import java.util.Objects;
import org.qwh.pms.core.storage.SSTMeta;

final class LocalRun {
    private final SSTMeta meta;

    LocalRun(SSTMeta meta) {
        this.meta = Objects.requireNonNull(meta, "meta must not be null");
    }

    SSTMeta meta() {
        return meta;
    }

    long runId() {
        return meta.runId();
    }

    long oldestWriteAtMillis() {
        return meta.oldestWriteAtMillis();
    }

    LocalRunSnapshot snapshot(long nowMillis) {
        return new LocalRunSnapshot(
            meta.runId(),
            meta.minFlushId(),
            meta.maxFlushId(),
            meta.state(),
            meta.fileSize(),
            meta.entryCount(),
            meta.minSequenceId(),
            meta.maxSequenceId(),
            meta.oldestWriteAtMillis(),
            ageMillis(nowMillis, meta.oldestWriteAtMillis())
        );
    }

    private static long ageMillis(long nowMillis, long oldestWriteAtMillis) {
        if (oldestWriteAtMillis <= 0) {
            return 0;
        }
        return Math.max(0, nowMillis - oldestWriteAtMillis);
    }
}
