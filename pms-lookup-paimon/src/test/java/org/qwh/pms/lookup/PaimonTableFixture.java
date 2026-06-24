package org.qwh.pms.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.qwh.pms.lookup.api.DataFileResolver;
import org.qwh.pms.lookup.api.ResolvedDataFile;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Test fixture for creating real Paimon primary-key tables and exposing commit deltas. */
final class PaimonTableFixture implements AutoCloseable {

    private final FileIO fileIO;
    private final Path tablePath;
    private final FileStoreTable table;

    private PaimonTableFixture(FileIO fileIO, Path tablePath, FileStoreTable table) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.table = table;
    }

    static PaimonTableFixture createPrimaryKeyTable(
            String tableName, RowType rowType, List<String> primaryKeys) throws Exception {
        return createPrimaryKeyTable(
                targetFixturePath(tableName),
                rowType,
                Collections.emptyList(),
                primaryKeys,
                1,
                Collections.emptyMap());
    }

    static PaimonTableFixture createPrimaryKeyTable(
            java.nio.file.Path tablePath, RowType rowType, List<String> primaryKeys)
            throws Exception {
        return createPrimaryKeyTable(tablePath, rowType, primaryKeys, Collections.emptyMap());
    }

    static PaimonTableFixture createPrimaryKeyTable(
            java.nio.file.Path tablePath,
            RowType rowType,
            List<String> primaryKeys,
            Map<String, String> extraOptions)
            throws Exception {
        return createPrimaryKeyTable(
                tablePath, rowType, Collections.emptyList(), primaryKeys, 1, extraOptions);
    }

    static PaimonTableFixture createPrimaryKeyTable(
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int bucketCount)
            throws Exception {
        return createPrimaryKeyTable(
                targetFixturePath(tableName),
                rowType,
                partitionKeys,
                primaryKeys,
                bucketCount,
                Collections.emptyMap());
    }

    static PaimonTableFixture createPrimaryKeyTable(
            java.nio.file.Path tablePath,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int bucketCount,
            Map<String, String> extraOptions)
            throws Exception {
        Files.createDirectories(tablePath.getParent());
        FileIO fileIO = LocalFileIO.create();
        Path paimonTablePath = new Path(tablePath.toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, paimonTablePath.toString());
        options.set(CoreOptions.BUCKET, bucketCount);
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        extraOptions.forEach(options::setString);

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        partitionKeys,
                        primaryKeys,
                        options.toMap(),
                        "");
        new SchemaManager(fileIO, paimonTablePath).createTable(schema);
        return new PaimonTableFixture(
                fileIO, paimonTablePath, FileStoreTableFactory.create(fileIO, options));
    }

    WriteResult writeRows(List<? extends InternalRow> rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        List<CommitMessage> messages;
        try (BatchTableWrite write = builder.newWrite()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            messages = write.prepareCommit();
        }

        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
        }

        return WriteResult.fromCommitMessages(messages);
    }

    WriteResult compact(BinaryRow partition, int bucket) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        List<CommitMessage> messages;
        try (BatchTableWrite write = builder.newWrite()) {
            write.compact(partition, bucket, true);
            messages = write.prepareCommit();
        }

        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
        }

        return WriteResult.fromCommitMessages(messages);
    }

    int bucketOf(InternalRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            return write.getBucket(row);
        }
    }

    DataFileResolver dataFileResolver() {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        return (context, file) -> {
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(context.partition(), context.bucket());
            Path dataPath = dataFilePathFactory.toPath(file);
            return new ResolvedDataFile(fileIO, dataPath, fileIO.getFileSize(dataPath));
        };
    }

    Path tablePath() {
        return tablePath;
    }

    FileStoreTable table() {
        return table;
    }

    long schemaId() {
        return table.schema().id();
    }

    @Override
    public void close() {
        // Keep generated Paimon tables under target/ for manual inspection after tests.
        // They are removed by mvn clean.
    }

    private static java.nio.file.Path targetFixturePath(String tableName) {
        String runId = Long.toUnsignedString(System.nanoTime());
        return java.nio.file.Paths.get("target", "paimon-fixtures", tableName + "-" + runId)
                .toAbsolutePath();
    }

    record WriteResult(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<CommitMessage> commitMessages) {

        List<DataFileMeta> removedFiles() {
            List<DataFileMeta> files = new ArrayList<>(deletedFiles.size() + compactBefore.size());
            files.addAll(deletedFiles);
            files.addAll(compactBefore);
            return List.copyOf(files);
        }

        List<DataFileMeta> addedFiles() {
            List<DataFileMeta> files = new ArrayList<>(newFiles.size() + compactAfter.size());
            files.addAll(newFiles);
            files.addAll(compactAfter);
            return List.copyOf(files);
        }

        List<BucketDelta> bucketDeltas() {
            return commitMessages.stream().map(BucketDelta::fromCommitMessage).toList();
        }

        static WriteResult fromCommitMessages(List<CommitMessage> messages) {
            List<DataFileMeta> newFiles = new ArrayList<>();
            List<DataFileMeta> deletedFiles = new ArrayList<>();
            List<DataFileMeta> compactBefore = new ArrayList<>();
            List<DataFileMeta> compactAfter = new ArrayList<>();

            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                DataIncrement dataIncrement = impl.newFilesIncrement();
                CompactIncrement compactIncrement = impl.compactIncrement();
                newFiles.addAll(dataIncrement.newFiles());
                deletedFiles.addAll(dataIncrement.deletedFiles());
                compactBefore.addAll(compactIncrement.compactBefore());
                compactAfter.addAll(compactIncrement.compactAfter());
            }

            return new WriteResult(
                    List.copyOf(newFiles),
                    List.copyOf(deletedFiles),
                    List.copyOf(compactBefore),
                    List.copyOf(compactAfter),
                    List.copyOf(messages));
        }
    }

    record BucketDelta(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter) {

        List<DataFileMeta> removedFiles() {
            List<DataFileMeta> files = new ArrayList<>(deletedFiles.size() + compactBefore.size());
            files.addAll(deletedFiles);
            files.addAll(compactBefore);
            return List.copyOf(files);
        }

        List<DataFileMeta> addedFiles() {
            List<DataFileMeta> files = new ArrayList<>(newFiles.size() + compactAfter.size());
            files.addAll(newFiles);
            files.addAll(compactAfter);
            return List.copyOf(files);
        }

        private static BucketDelta fromCommitMessage(CommitMessage message) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            DataIncrement dataIncrement = impl.newFilesIncrement();
            CompactIncrement compactIncrement = impl.compactIncrement();
            return new BucketDelta(
                    impl.partition().copy(),
                    impl.bucket(),
                    List.copyOf(dataIncrement.newFiles()),
                    List.copyOf(dataIncrement.deletedFiles()),
                    List.copyOf(compactIncrement.compactBefore()),
                    List.copyOf(compactIncrement.compactAfter()));
        }
    }
}
