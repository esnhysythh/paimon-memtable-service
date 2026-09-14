package org.qwh.pms.benchmark.core;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.SSTMeta;

/** Local deterministic sink used only by db_bench scenarios that need SINKED runs. */
final class BenchmarkSinkManager implements SinkManager {
    private final AtomicLong nextSnapshotId = new AtomicLong(1);

    @Override
    public PreparedSinkCommit prepare(SinkBatch batch) {
        List<Long> runIds = batch.ssts().stream().map(SSTMeta::runId).toList();
        long rows = batch.ssts().stream().mapToLong(SSTMeta::entryCount).sum();
        return new PreparedSinkCommit(
            batch.batchId(),
            batch.maxSequenceId(),
            runIds,
            batch.minSequenceId(),
            batch.maxSequenceId(),
            ("benchmark-commit:" + batch.batchId()).getBytes(StandardCharsets.UTF_8),
            List.of(),
            rows,
            rows
        );
    }

    @Override
    public SinkCommitResult commit(PreparedSinkCommit prepared) {
        return new SinkCommitResult(
            prepared.batchId(),
            nextSnapshotId.getAndIncrement(),
            prepared.maxSequenceId(),
            prepared.sstIds(),
            new byte[0]
        );
    }
}
