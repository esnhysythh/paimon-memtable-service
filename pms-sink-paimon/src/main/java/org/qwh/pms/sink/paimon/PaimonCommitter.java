package org.qwh.pms.sink.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkFileRef;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class PaimonCommitter {
    private final Table table;
    private final String commitUser;
    private final PaimonCommitPayloadCodec payloadCodec;

    public PaimonCommitter(Table table, String commitUser) {
        this(table, commitUser, new PaimonCommitPayloadCodec());
    }

    PaimonCommitter(Table table, String commitUser, PaimonCommitPayloadCodec payloadCodec) {
        PaimonSinkTableValidator.validate(table);
        this.table = table;
        this.commitUser = commitUser;
        this.payloadCodec = payloadCodec;
    }

    public SinkCommitResult commit(PreparedSinkCommit prepared) {
        try {
            verifyPreparedFiles(prepared.fileRefs());
            List<CommitMessage> messages = payloadCodec.decode(prepared.payload());
            StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
            try (StreamTableCommit commit = builder.newCommit()) {
                commit.filterAndCommit(Map.of(prepared.commitIdentifier(), messages));
            }
            long snapshotId = table.latestSnapshot()
                .orElseThrow(() -> new IllegalStateException("Paimon commit produced no snapshot"))
                .id();
            return new SinkCommitResult(
                prepared.batchId(),
                snapshotId,
                prepared.maxSequenceId(),
                prepared.sstIds()
            );
        } catch (Exception e) {
            throw new RuntimeException("Paimon commit failed for batch " + prepared.batchId(), e);
        }
    }

    private void verifyPreparedFiles(List<SinkFileRef> refs) throws IOException {
        FileIO fileIO = table.fileIO();
        for (SinkFileRef ref : refs) {
            Path path = new Path(ref.path());
            long actualSize = fileIO.getFileStatus(path).getLen();
            if (actualSize != ref.fileSize()) {
                throw new IllegalStateException(
                    "Prepared file changed: "
                        + ref.fileName()
                        + ", expected size="
                        + ref.fileSize()
                        + ", actual size="
                        + actualSize
                );
            }
        }
    }
}
