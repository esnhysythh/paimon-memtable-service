package org.qwh.pms.core.sink;

public interface SinkManager {

    PreparedSinkCommit prepare(SinkBatch batch);

    SinkCommitResult commit(PreparedSinkCommit prepared);
}
