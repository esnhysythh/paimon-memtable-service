package org.qwh.pms.benchmark.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.qwh.pms.lookup.api.DataFileResolver;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.cache.LocalCacheDirectory;
import org.qwh.pms.lookup.cache.valuesst.ValueSstCacheBuilder;

import java.nio.file.Path;
import java.util.List;

/** Real committed, single-file fixture. All writing happens outside query measurements. */
final class LocalPaimonData {
    static final RowType ROW_TYPE = RowType.builder()
        .field("id", DataTypes.INT().notNull()).field("payload", DataTypes.STRING()).build();
    static final RowType KEY_TYPE = PrimaryKeyTableUtils.addKeyNamePrefix(ROW_TYPE.project(new int[]{0}));
    final FileStoreTable table;
    final LocalFileIO fileIO;
    final List<DataFileMeta> files;
    final int rows;
    final int valueSize;

    LocalPaimonData(Path directory, int rows, int valueSize) throws Exception {
        this.rows = rows;
        this.valueSize = valueSize;
        fileIO = LocalFileIO.create();
        var path = new org.apache.paimon.fs.Path(directory.toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, path.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        options.setString("write-buffer-size", "256mb");
        options.setString("target-file-size", "256mb");
        new SchemaManager(fileIO, path).createTable(
            new Schema(ROW_TYPE.getFields(), List.of(), List.of("id"), options.toMap(), ""));
        table = FileStoreTableFactory.create(fileIO, options);
        var builder = table.newBatchWriteBuilder();
        List<CommitMessage> messages;
        try (var write = builder.newWrite()) {
            for (int i = 0; i < rows; i++) {
                int key = i * 2;
                write.write(GenericRow.of(key, BinaryString.fromString(payload(key, valueSize))));
            }
            messages = write.prepareCommit();
        }
        try (var commit = builder.newCommit()) {
            commit.commit(messages);
        }
        files = table.store().newScan().withPartitionBucket(BinaryRow.EMPTY_ROW, 0)
            .plan().files(FileKind.ADD).stream().map(entry -> entry.file()).toList();
        // Fix file layout rather than accidentally comparing different candidate counts.
        if (files.size() != 1 || files.get(0).rowCount() != rows) {
            throw new IllegalStateException("Single-file baseline required; reduce --num or --value_size: files=" + files.size());
        }
    }

    static String payload(int key, int length) {
        char[] chars = new char[length];
        long state = key + 0x9e3779b97f4a7c15L;
        for (int i = 0; i < chars.length; i++) {
            state ^= state >>> 12;
            state ^= state << 25;
            state ^= state >>> 27;
            chars[i] = (char) ('a' + Long.remainderUnsigned(state * 2685821657736338717L, 26));
        }
        return new String(chars);
    }

    DataFileResolver resolver() {
        return (context, file) -> {
            var path = table.store().pathFactory()
                .createDataFilePathFactory(context.partition(), context.bucket()).toPath(file);
            return new ResolvedDataFile(fileIO, path, file.fileSize());
        };
    }

    ValueSstCacheBuilder cacheBuilder(LocalCacheDirectory directory) {
        CoreOptions options = table.coreOptions();
        var store = LookupStoreFactory.create(options,
            new CacheManager(options.lookupCacheMaxMemory(), options.lookupCacheHighPrioPoolRatio()),
            new RowCompactedSerializer(KEY_TYPE).createSliceComparator());
        return new ValueSstCacheBuilder(KEY_TYPE, ROW_TYPE,
            (file, context) -> ((KeyValueFileStore) table.store()).newReaderFactoryBuilder()
                .build(context.partition(), context.bucket(), DeletionVector.emptyFactory())
                .createRecordReader(file),
            store, LookupSerializerFactory.INSTANCE.get(),
            LookupStoreFactory.bfGenerator(options.toConfiguration()), directory);
    }
}
