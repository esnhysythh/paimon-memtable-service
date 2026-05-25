package org.qwh.pms.sink.paimon;

import org.apache.paimon.table.Table;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.LocalStorageManager;

public final class PaimonSinkManager implements SinkManager {
    private final PaimonFlusher flusher;
    private final PaimonCommitter committer;

    public PaimonSinkManager(Table table, String commitUser, LocalStorageManager storageManager) {
        this(new PaimonFlusher(table, commitUser, storageManager), new PaimonCommitter(table, commitUser));
    }

    PaimonSinkManager(PaimonFlusher flusher, PaimonCommitter committer) {
        this.flusher = flusher;
        this.committer = committer;
    }

    @Override
    public PreparedSinkCommit prepare(SinkBatch batch) {
        return flusher.prepare(batch);
    }

    @Override
    public SinkCommitResult commit(PreparedSinkCommit prepared) {
        return committer.commit(prepared);
    }
}
