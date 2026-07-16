package org.qwh.pms.core.sink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.storage.SSTMeta;
import org.qwh.pms.core.storage.SSTState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkCoordinatorTest {

    @TempDir
    Path tempDir;

    @Test
    void rejectsPrepareThatDoesNotMatchSelectedBatchBeforePersistingMetadata() throws IOException {
        SinkBatch batch = batch();
        SinkMetaStore metaStore = metaStore();
        SinkCoordinator coordinator = new SinkCoordinator(new SinkManager() {
            @Override
            public PreparedSinkCommit prepare(SinkBatch ignored) {
                return prepared(batch, List.of(12L, 11L));
            }

            @Override
            public SinkCommitResult commit(PreparedSinkCommit prepared) {
                throw new AssertionError("commit must not run for an invalid prepare result");
            }
        }, metaStore);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> coordinator.sink(batch));

        assertTrue(error.getMessage().contains("prepare result does not match"));
        SinkRecoveryState recovery = metaStore.load();
        assertTrue(recovery.pendingPrepares().isEmpty());
        assertTrue(recovery.sinkedSSTIds().isEmpty());
    }

    @Test
    void rejectsCommitThatDoesNotMatchPrepareBeforePersistingSuccess() throws IOException {
        SinkBatch batch = batch();
        SinkMetaStore metaStore = metaStore();
        SinkCoordinator coordinator = new SinkCoordinator(new SinkManager() {
            @Override
            public PreparedSinkCommit prepare(SinkBatch selected) {
                return prepared(selected, selected.ssts().stream().map(SSTMeta::runId).toList());
            }

            @Override
            public SinkCommitResult commit(PreparedSinkCommit prepared) {
                return new SinkCommitResult(
                    prepared.batchId(),
                    1,
                    prepared.maxSequenceId(),
                    List.of(prepared.sstIds().get(0))
                );
            }
        }, metaStore);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> coordinator.sink(batch));

        assertTrue(error.getMessage().contains("commit result does not match"));
        SinkRecoveryState recovery = metaStore.load();
        assertEquals(1, recovery.pendingPrepares().size());
        assertEquals(batch.batchId(), recovery.pendingPrepares().get(0).batchId());
        assertTrue(recovery.sinkedSSTIds().isEmpty());
        assertEquals(0, recovery.lastPersistedSequenceId());
    }

    @Test
    void persistsSuccessOnlyAfterExactPrepareAndCommitResults() throws IOException {
        SinkBatch batch = batch();
        SinkMetaStore metaStore = metaStore();
        SinkCoordinator coordinator = new SinkCoordinator(new SinkManager() {
            @Override
            public PreparedSinkCommit prepare(SinkBatch selected) {
                return prepared(selected, selected.ssts().stream().map(SSTMeta::runId).toList());
            }

            @Override
            public SinkCommitResult commit(PreparedSinkCommit prepared) {
                return new SinkCommitResult(
                    prepared.batchId(),
                    7,
                    prepared.maxSequenceId(),
                    prepared.sstIds()
                );
            }
        }, metaStore);

        SinkCommitResult result = coordinator.sink(batch);

        assertEquals(List.of(11L, 12L), result.sstIds());
        SinkRecoveryState recovery = metaStore.load();
        assertTrue(recovery.pendingPrepares().isEmpty());
        assertEquals(Set.copyOf(result.sstIds()), recovery.sinkedSSTIds());
        assertEquals(2, recovery.lastPersistedSequenceId());
    }

    private SinkMetaStore metaStore() throws IOException {
        SinkMetaStore metaStore = new SinkMetaStore(tempDir.resolve("sink-meta"));
        metaStore.init();
        return metaStore;
    }

    private SinkBatch batch() {
        return new SinkBatch(
            "sink-2-2",
            List.of(sst(11, 1, 1), sst(12, 2, 2)),
            1,
            2
        );
    }

    private SSTMeta sst(long runId, long flushId, long sequenceId) {
        return new SSTMeta(
            runId,
            flushId,
            flushId,
            tempDir.resolve("sst-" + runId + ".sst"),
            0,
            0,
            null,
            null,
            sequenceId,
            sequenceId,
            0,
            0,
            SSTState.NEW
        );
    }

    private static PreparedSinkCommit prepared(SinkBatch batch, List<Long> sstIds) {
        return new PreparedSinkCommit(
            batch.batchId(),
            batch.maxSequenceId(),
            sstIds,
            batch.minSequenceId(),
            batch.maxSequenceId(),
            new byte[] {1},
            List.of(),
            batch.ssts().size(),
            batch.ssts().size()
        );
    }
}
