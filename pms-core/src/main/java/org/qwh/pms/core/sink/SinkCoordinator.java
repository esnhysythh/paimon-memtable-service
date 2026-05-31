package org.qwh.pms.core.sink;

import java.util.Objects;

public final class SinkCoordinator {
    private final SinkManager sinkManager;
    private final SinkMetaStore sinkMetaStore;

    public SinkCoordinator(SinkManager sinkManager, SinkMetaStore sinkMetaStore) {
        this.sinkManager = Objects.requireNonNull(sinkManager, "sinkManager must not be null");
        this.sinkMetaStore = Objects.requireNonNull(sinkMetaStore, "sinkMetaStore must not be null");
    }

    public SinkCommitResult sink(SinkBatch batch) {
        PreparedSinkCommit prepared = sinkManager.prepare(batch);
        sinkMetaStore.savePrepare(prepared);
        return commitPrepared(prepared);
    }

    public SinkCommitResult recoverPrepared(PreparedSinkCommit prepared) {
        return commitPrepared(prepared);
    }

    private SinkCommitResult commitPrepared(PreparedSinkCommit prepared) {
        SinkCommitResult result = sinkManager.commit(prepared);
        sinkMetaStore.saveSuccess(result);
        return result;
    }
}
