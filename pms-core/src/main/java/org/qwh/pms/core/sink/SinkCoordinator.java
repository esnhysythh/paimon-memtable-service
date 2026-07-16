package org.qwh.pms.core.sink;

import java.util.List;
import java.util.Objects;
import org.qwh.pms.core.storage.SSTMeta;

public final class SinkCoordinator {
    private final SinkManager sinkManager;
    private final SinkMetaStore sinkMetaStore;

    public SinkCoordinator(SinkManager sinkManager, SinkMetaStore sinkMetaStore) {
        this.sinkManager = Objects.requireNonNull(sinkManager, "sinkManager must not be null");
        this.sinkMetaStore = Objects.requireNonNull(sinkMetaStore, "sinkMetaStore must not be null");
    }

    public SinkCommitResult sink(SinkBatch batch) {
        PreparedSinkCommit prepared = Objects.requireNonNull(
            sinkManager.prepare(batch),
            "SinkManager.prepare must not return null"
        );
        validatePreparedMatchesBatch(batch, prepared);
        sinkMetaStore.savePrepare(prepared);
        return commitPrepared(prepared);
    }

    public SinkCommitResult recoverPrepared(PreparedSinkCommit prepared) {
        return commitPrepared(prepared);
    }

    private SinkCommitResult commitPrepared(PreparedSinkCommit prepared) {
        SinkCommitResult result = Objects.requireNonNull(
            sinkManager.commit(prepared),
            "SinkManager.commit must not return null"
        );
        validateCommitMatchesPrepare(prepared, result);
        sinkMetaStore.saveSuccess(result);
        return result;
    }

    private static void validatePreparedMatchesBatch(SinkBatch batch, PreparedSinkCommit prepared) {
        List<Long> selectedRunIds = batch.ssts().stream().map(SSTMeta::runId).toList();
        if (!prepared.batchId().equals(batch.batchId())
                || !prepared.sstIds().equals(selectedRunIds)
                || prepared.minSequenceId() != batch.minSequenceId()
                || prepared.maxSequenceId() != batch.maxSequenceId()) {
            throw new IllegalStateException(
                "Sink prepare result does not match selected batch: batch=" + batch.batchId()
            );
        }
    }

    private static void validateCommitMatchesPrepare(
            PreparedSinkCommit prepared,
            SinkCommitResult result) {
        if (!result.batchId().equals(prepared.batchId())
                || !result.sstIds().equals(prepared.sstIds())
                || result.persistedSequenceId() != prepared.maxSequenceId()) {
            throw new IllegalStateException(
                "Sink commit result does not match prepared batch: batch=" + prepared.batchId()
            );
        }
    }
}
