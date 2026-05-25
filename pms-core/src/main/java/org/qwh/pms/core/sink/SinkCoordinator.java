package org.qwh.pms.core.sink;

import org.qwh.pms.core.wal.WALManager;

import java.util.Objects;

public final class SinkCoordinator {
    private final SinkManager sinkManager;
    private final WALManager walManager;

    public SinkCoordinator(SinkManager sinkManager, WALManager walManager) {
        this.sinkManager = Objects.requireNonNull(sinkManager, "sinkManager must not be null");
        this.walManager = Objects.requireNonNull(walManager, "walManager must not be null");
    }

    public SinkCommitResult sink(SinkBatch batch) {
        PreparedSinkCommit prepared = sinkManager.prepare(batch);
        walManager.appendSinkPrepare(SinkWalCodec.encodePrepare(prepared));
        return commitPrepared(prepared);
    }

    public SinkCommitResult recoverPrepared(PreparedSinkCommit prepared) {
        return commitPrepared(prepared);
    }

    private SinkCommitResult commitPrepared(PreparedSinkCommit prepared) {
        SinkCommitResult result = sinkManager.commit(prepared);
        walManager.appendSinkSuccess(result.snapshotId(), SinkWalCodec.encodeSuccess(result));
        return result;
    }
}
