package org.qwh.pms.core.sink;

import org.qwh.pms.core.storage.SSTMeta;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MockSinkManager implements SinkManager {
    private final AtomicLong nextSnapshotId = new AtomicLong(1);

    @Override
    public PreparedSinkCommit prepare(SinkBatch batch) {
        List<Long> sstIds = batch.ssts().stream().map(SSTMeta::runId).toList();
        long inputRecordCount = batch.ssts().stream().mapToLong(SSTMeta::entryCount).sum();
        byte[] payload = ("mock-paimon-commit:" + batch.batchId()).getBytes(StandardCharsets.UTF_8);
        return new PreparedSinkCommit(
            batch.batchId(),
            batch.maxSequenceId(),
            sstIds,
            batch.minSequenceId(),
            batch.maxSequenceId(),
            payload,
            List.of(),
            inputRecordCount,
            inputRecordCount
        );
    }

    @Override
    public SinkCommitResult commit(PreparedSinkCommit prepared) {
        return new SinkCommitResult(
            prepared.batchId(),
            nextSnapshotId.getAndIncrement(),
            prepared.maxSequenceId(),
            prepared.sstIds()
        );
    }
}
