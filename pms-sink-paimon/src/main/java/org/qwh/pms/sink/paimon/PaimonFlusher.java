package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkFileRef;
import org.qwh.pms.core.storage.LocalStorageManager;
import org.qwh.pms.core.storage.SSTEntryIterator;
import org.qwh.pms.core.storage.SSTMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PaimonFlusher {
    private final Table table;
    private final String commitUser;
    private final LocalStorageManager storageManager;
    private final PaimonSinkEntryMerger merger;
    private final PaimonSinkRowConverter rowConverter;
    private final PaimonCommitPayloadCodec payloadCodec;

    public PaimonFlusher(Table table, String commitUser, LocalStorageManager storageManager) {
        this(
            validateTable(table),
            commitUser,
            storageManager,
            new PaimonSinkEntryMerger(),
            new PaimonSinkRowConverter(table.rowType(), table.primaryKeys()),
            new PaimonCommitPayloadCodec()
        );
    }

    PaimonFlusher(
            Table table,
            String commitUser,
            LocalStorageManager storageManager,
            PaimonSinkEntryMerger merger,
            PaimonSinkRowConverter rowConverter,
            PaimonCommitPayloadCodec payloadCodec) {
        PaimonSinkTableValidator.validate(table);
        this.table = table;
        this.commitUser = commitUser;
        this.storageManager = storageManager;
        this.merger = merger;
        this.rowConverter = rowConverter;
        this.payloadCodec = payloadCodec;
    }

    private static Table validateTable(Table table) {
        PaimonSinkTableValidator.validate(table);
        return table;
    }

    public PreparedSinkCommit prepare(SinkBatch batch) {
        try {
            List<SSTEntryIterator> inputs = openIterators(batch.ssts());
            long outputRecordCount = 0;
            List<CommitMessage> messages;
            StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
            try (SSTEntryIterator merged = merger.mergeLatest(inputs);
                 StreamTableWrite write = builder.newWrite()) {
                while (merged.hasNext()) {
                    Entry entry = merged.next();
                    InternalRow row = rowConverter.toPaimonRow(entry);
                    write.write(row);
                    outputRecordCount++;
                }
                messages = write.prepareCommit(true, commitIdentifier(batch));
            }

            return new PreparedSinkCommit(
                batch.batchId(),
                commitIdentifier(batch),
                sstIds(batch.ssts()),
                batch.minSequenceId(),
                batch.maxSequenceId(),
                payloadCodec.encode(messages),
                collectFileRefs(messages),
                inputRecordCount(batch.ssts()),
                outputRecordCount
            );
        } catch (Exception e) {
            throw new RuntimeException("Paimon prepare failed for batch " + batch.batchId(), e);
        }
    }

    private List<SSTEntryIterator> openIterators(List<SSTMeta> ssts) {
        List<SSTEntryIterator> inputs = new ArrayList<>(ssts.size());
        try {
            for (SSTMeta sst : ssts) {
                inputs.add(storageManager.openIterator(sst));
            }
            return inputs;
        } catch (RuntimeException e) {
            closeAll(inputs, e);
            throw e;
        }
    }

    private static void closeAll(List<SSTEntryIterator> inputs, RuntimeException owner) {
        for (SSTEntryIterator input : inputs) {
            try {
                input.close();
            } catch (RuntimeException e) {
                owner.addSuppressed(e);
            }
        }
    }

    private List<SinkFileRef> collectFileRefs(List<CommitMessage> messages) throws IOException {
        List<SinkFileRef> refs = new ArrayList<>();
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileIO fileIO = table.fileIO();
        for (CommitMessage message : messages) {
            DataFilePathFactory pathFactory =
                fileStoreTable.store().pathFactory().createDataFilePathFactory(
                    message.partition(),
                    message.bucket()
                );
            for (DataFileMeta file : dataFiles(message)) {
                Path path = pathFactory.toPath(file);
                refs.add(new SinkFileRef(
                    file.fileName(),
                    path.toString(),
                    fileIO.getFileStatus(path).getLen(),
                    file.rowCount(),
                    message.partition().toString(),
                    message.bucket()
                ));
            }
        }
        return refs;
    }

    private static List<DataFileMeta> dataFiles(CommitMessage message) {
        CommitMessageImpl impl = (CommitMessageImpl) message;
        DataIncrement dataIncrement = impl.newFilesIncrement();
        CompactIncrement compactIncrement = impl.compactIncrement();
        List<DataFileMeta> files = new ArrayList<>();
        files.addAll(dataIncrement.newFiles());
        files.addAll(dataIncrement.changelogFiles());
        files.addAll(compactIncrement.compactAfter());
        files.addAll(compactIncrement.changelogFiles());
        return files;
    }

    private static long commitIdentifier(SinkBatch batch) {
        return batch.maxSequenceId();
    }

    private static List<Long> sstIds(List<SSTMeta> ssts) {
        return ssts.stream().map(SSTMeta::fileId).toList();
    }

    private static long inputRecordCount(List<SSTMeta> ssts) {
        return ssts.stream().mapToLong(SSTMeta::entryCount).sum();
    }
}
