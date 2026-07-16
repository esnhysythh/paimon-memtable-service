package org.qwh.pms.sink.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.SkipListCurMemTable;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkMetaPayloadCodec;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.core.storage.LocalStorageManager;
import org.qwh.pms.core.storage.SSTEntryIterator;
import org.qwh.pms.core.storage.SSTMeta;
import org.qwh.pms.core.storage.SSTReadSnapshot;
import org.qwh.pms.core.storage.SSTState;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSinkManagerIntegrationTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void preparesCommitsReplaysAndDeletesThroughPaimon() throws Exception {
        try (TestTable testTable = createTable()) {
            RowType rowType = testTable.table().rowType();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta first = storage.flushToSST(immutable(
                put(rowType, 1, "old-a", 1),
                put(rowType, 2, "old-b", 2)
            ));
            PreparedSinkCommit preparedFirst = prepare(sinkManager, "batch-001", first);
            assertFalse(preparedFirst.fileRefs().isEmpty());

            PreparedSinkCommit replayedFirst =
                SinkMetaPayloadCodec.decodePrepare(SinkMetaPayloadCodec.encodePrepare(preparedFirst));
            SinkCommitResult firstResult = sinkManager.commit(replayedFirst);
            assertEquals(first.maxSequenceId(), firstResult.persistedSequenceId());
            assertEquals(Map.of(1, "old-a", 2, "old-b"), readRows(testTable.table()));

            SinkCommitResult duplicateResult = sinkManager.commit(replayedFirst);
            assertEquals(firstResult.snapshotId(), duplicateResult.snapshotId());
            assertEquals(Map.of(1, "old-a", 2, "old-b"), readRows(testTable.table()));

            SSTMeta second = storage.flushToSST(immutable(
                put(rowType, 1, "new-a", 3),
                delete(rowType, 2, 4)
            ));
            PreparedSinkCommit preparedSecond = prepare(sinkManager, "batch-002", second);
            SinkCommitResult secondResult = sinkManager.commit(
                SinkMetaPayloadCodec.decodePrepare(SinkMetaPayloadCodec.encodePrepare(preparedSecond))
            );

            assertTrue(secondResult.snapshotId() > firstResult.snapshotId());
            assertEquals(second.maxSequenceId(), secondResult.persistedSequenceId());
            assertEquals(Map.of(1, "new-a"), readRows(testTable.table()));
        }
    }

    @Test
    void deletesNotNullNonPrimaryKeyRowsWithSyntheticPayload() throws Exception {
        try (TestTable testTable = createTable(notNullSchema())) {
            RowType rowType = testTable.table().rowType();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta first = storage.flushToSST(immutable(put(rowType, 1, "old-a", 1)));
            sinkManager.commit(prepare(sinkManager, "batch-001", first));
            assertEquals(Map.of(1, "old-a"), readRows(testTable.table()));

            SSTMeta second = storage.flushToSST(immutable(delete(rowType, 1, 2)));
            PreparedSinkCommit preparedSecond = prepare(sinkManager, "batch-002", second);
            SinkCommitResult secondResult = sinkManager.commit(preparedSecond);

            assertEquals(second.maxSequenceId(), secondResult.persistedSequenceId());
            assertEquals(Map.of(), readRows(testTable.table()));
        }
    }

    @Test
    void writesDeleteWithNotNullComplexSyntheticPayload() throws Exception {
        try (TestTable testTable = createTable(notNullComplexSchema())) {
            RowType rowType = testTable.table().rowType();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta meta = storage.flushToSST(immutable(delete(rowType, 1, 1)));
            SinkCommitResult result = sinkManager.commit(prepare(sinkManager, "batch-complex", meta));

            assertEquals(meta.maxSequenceId(), result.persistedSequenceId());
            assertEquals(Map.of(), readRows(testTable.table()));
        }
    }

    @Test
    void mergesMultipleSstsBeforeWritingToPaimon() throws Exception {
        try (TestTable testTable = createTable()) {
            RowType rowType = testTable.table().rowType();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta first = storage.flushToSST(immutable(
                put(rowType, 1, "old-a", 1),
                put(rowType, 2, "old-b", 2)
            ));
            SSTMeta second = storage.flushToSST(immutable(
                put(rowType, 1, "new-a", 5),
                put(rowType, 3, "new-c", 4)
            ));

            PreparedSinkCommit prepared = prepare(sinkManager, "batch-merged", List.of(first, second));
            assertEquals(4, prepared.inputRecordCount());
            assertEquals(3, prepared.outputRecordCount());
            sinkManager.commit(prepared);

            assertEquals(Map.of(1, "new-a", 2, "old-b", 3, "new-c"), readRows(testTable.table()));
        }
    }

    @Test
    void writesPartitionedTableWithCompositePrimaryKeyAndDelete() throws Exception {
        try (TestTable testTable = createTable(partitionedCompositeSchema())) {
            RowType rowType = testTable.table().rowType();
            List<String> primaryKeys = testTable.table().primaryKeys();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta first = storage.flushToSST(immutable(
                putComposite(rowType, primaryKeys, "2026-05-26", 1, "old-a", 1),
                putComposite(rowType, primaryKeys, "2026-05-26", 2, "old-b", 2)
            ));
            SSTMeta second = storage.flushToSST(immutable(
                putComposite(rowType, primaryKeys, "2026-05-27", 1, "new-a", 3),
                deleteComposite(rowType, primaryKeys, "2026-05-26", 2, 4)
            ));

            PreparedSinkCommit prepared = prepare(sinkManager, "batch-partitioned", List.of(first, second));
            assertEquals(4, prepared.inputRecordCount());
            assertEquals(3, prepared.outputRecordCount());
            sinkManager.commit(prepared);

            assertEquals(
                Map.of(
                    "2026-05-26#1", "old-a",
                    "2026-05-27#1", "new-a"
                ),
                readCompositeRows(testTable.table())
            );
        }
    }

    @Test
    void rejectsCommitWhenPreparedDataFileIsMissing() throws Exception {
        try (TestTable testTable = createTable()) {
            RowType rowType = testTable.table().rowType();
            FileLocalStorageManager storage = storage();
            PaimonSinkManager sinkManager =
                new PaimonSinkManager(testTable.table(), "pms-test", storage);

            SSTMeta first = storage.flushToSST(immutable(put(rowType, 1, "old-a", 1)));
            PreparedSinkCommit prepared = prepare(sinkManager, "batch-missing-file", first);
            assertFalse(prepared.fileRefs().isEmpty());

            Path path = new Path(prepared.fileRefs().get(0).path());
            assertTrue(testTable.table().fileIO().delete(path, false));

            RuntimeException error =
                assertThrows(RuntimeException.class, () -> sinkManager.commit(prepared));
            assertTrue(error.getMessage().contains("Paimon commit failed for batch batch-missing-file"));
        }
    }

    @Test
    void rejectsUnsupportedTablesDuringConstruction() throws Exception {
        try (TestTable noPrimaryKeyTable = createTable(nonPrimaryKeySchema())) {
            RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> new PaimonSinkManager(noPrimaryKeyTable.table(), "pms-test", storage())
            );
            assertTrue(error.getMessage().contains("primary-key table"), error::getMessage);
        }

        Table partialUpdateTable = unsupportedMergeEngineTable();
        RuntimeException error = assertThrows(
            RuntimeException.class,
            () -> new PaimonSinkManager(partialUpdateTable, "pms-test", storage())
        );
        assertTrue(error.getMessage().contains("merge-engine=deduplicate"), error::getMessage);

        Table dynamicBucketTable = unsupportedBucketModeTable(BucketMode.HASH_DYNAMIC);
        RuntimeException dynamicBucketError = assertThrows(
            RuntimeException.class,
            () -> new PaimonSinkManager(dynamicBucketTable, "pms-test", storage())
        );
        assertTrue(dynamicBucketError.getMessage().contains("HASH_FIXED"), dynamicBucketError::getMessage);

        Table crossPartitionTable = crossPartitionTable();
        RuntimeException crossPartitionError = assertThrows(
            RuntimeException.class,
            () -> new PaimonSinkManager(crossPartitionTable, "pms-test", storage())
        );
        assertTrue(crossPartitionError.getMessage().contains("Cross Partitions Upsert"),
            crossPartitionError::getMessage);
    }

    @Test
    void closesOpenedIteratorsWhenPrepareCannotOpenLaterSst() throws Exception {
        try (TestTable testTable = createTable()) {
            TrackingIterator opened = new TrackingIterator();
            FailingSnapshotStorage storage = new FailingSnapshotStorage(opened);
            PaimonSinkManager sinkManager = new PaimonSinkManager(
                testTable.table(),
                "pms-test",
                storage
            );

            RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> sinkManager.prepare(new SinkBatch(
                    "batch-open-fails",
                    List.of(fakeMeta(1, 1), fakeMeta(2, 2)),
                    1,
                    2
                ))
            );

            assertTrue(opened.closed);
            assertTrue(storage.snapshotClosed);
            assertTrue(error.getMessage().contains("Paimon prepare failed for batch batch-open-fails"));
            assertTrue(error.getCause().getMessage().contains("open failed"));
        }
    }

    private PreparedSinkCommit prepare(PaimonSinkManager sinkManager, String batchId, SSTMeta meta) {
        return prepare(sinkManager, batchId, List.of(meta));
    }

    private PreparedSinkCommit prepare(PaimonSinkManager sinkManager, String batchId, List<SSTMeta> metas) {
        return sinkManager.prepare(new SinkBatch(
            batchId,
            metas,
            metas.stream().mapToLong(SSTMeta::minSequenceId).min().orElseThrow(),
            metas.stream().mapToLong(SSTMeta::maxSequenceId).max().orElseThrow()
        ));
    }

    private FileLocalStorageManager storage() throws IOException {
        FileLocalStorageManager storage = new FileLocalStorageManager(
            new StorageConfig(tempDir.resolve("storage").toString(), 0, 0, 0, 0)
        );
        storage.init();
        return storage;
    }

    private ImmutableMemTable immutable(TestEntry... entries) {
        SkipListCurMemTable cur = new SkipListCurMemTable(new MemTableConfig(1_000_000, 256));
        for (TestEntry entry : entries) {
            cur.put(entry.key(), entry.value());
        }
        return cur.freeze();
    }

    private TestEntry put(RowType rowType, int id, String marker, long sequenceId) {
        GenericRow row = row(id, marker);
        PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id"));
        PmsRowValueCodec valueCodec = new PmsRowValueCodec();
        return new TestEntry(
            new Key(keyCodec.encodeKey(row)),
            new Value(valueCodec.encode(rowType, row, 0), sequenceId)
        );
    }

    private TestEntry delete(RowType rowType, int id, long sequenceId) {
        GenericRow row = new GenericRow(rowType.getFieldCount());
        row.setField(0, id);
        PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id"));
        return new TestEntry(new Key(keyCodec.encodeKey(row)), Value.tombstone(sequenceId));
    }

    private TestEntry putComposite(
            RowType rowType,
            List<String> primaryKeys,
            String day,
            int id,
            String marker,
            long sequenceId) {
        GenericRow row = compositeRow(day, id, marker);
        PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeys);
        PmsRowValueCodec valueCodec = new PmsRowValueCodec();
        return new TestEntry(
            new Key(keyCodec.encodeKey(row)),
            new Value(valueCodec.encode(rowType, row, 0), sequenceId)
        );
    }

    private TestEntry deleteComposite(
            RowType rowType, List<String> primaryKeys, String day, int id, long sequenceId) {
        GenericRow row = compositeRow(day, id, "deleted");
        PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeys);
        return new TestEntry(new Key(keyCodec.encodeKey(row)), Value.tombstone(sequenceId));
    }

    private static GenericRow row(int id, String marker) {
        return GenericRow.ofKind(RowKind.INSERT, id, BinaryString.fromString(marker));
    }

    private static GenericRow compositeRow(String day, int id, String marker) {
        return GenericRow.ofKind(
            RowKind.INSERT,
            BinaryString.fromString(day),
            id,
            BinaryString.fromString(marker)
        );
    }

    private TestTable createTable() throws Exception {
        return createTable(schema());
    }

    private TestTable createTable(Schema schema) throws Exception {
        java.nio.file.Path warehouse = tempDir.resolve("warehouse");
        Catalog catalog = CatalogFactory.createCatalog(
            CatalogContext.create(new Path(warehouse.toUri().toString()))
        );
        catalog.createDatabase("pms_db", true);
        Identifier identifier = Identifier.create("pms_db", "sink_pk");
        catalog.createTable(identifier, schema, true);
        return new TestTable(catalog, catalog.getTable(identifier));
    }

    private static Schema schema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static Schema notNullSchema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING().notNull())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static Schema notNullComplexSchema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("items", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull())
            .column(
                "attributes",
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT().notNull()).notNull()
            )
            .column(
                "details",
                DataTypes.ROW(
                    DataTypes.FIELD(31, "required", DataTypes.STRING().notNull()),
                    DataTypes.FIELD(32, "optional", DataTypes.INT())
                ).notNull()
            )
            .column("amount", DataTypes.DECIMAL(38, 18).notNull())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static Schema partitionedCompositeSchema() {
        return Schema.newBuilder()
            .column("day", DataTypes.STRING())
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .partitionKeys("day")
            .primaryKey("day", "id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static Schema nonPrimaryKeySchema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static Map<Integer, String> readRows(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        Map<Integer, String> rows = new TreeMap<>();
        try (RecordReader<InternalRow> reader =
                 readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.put(row.getInt(0), row.getString(1).toString()));
        }
        return rows;
    }

    private static Map<String, String> readCompositeRows(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        Map<String, String> rows = new TreeMap<>();
        try (RecordReader<InternalRow> reader =
                 readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.put(
                row.getString(0).toString() + "#" + row.getInt(1),
                row.getString(2).toString()
            ));
        }
        return rows;
    }

    private static SSTMeta fakeMeta(long runId, long sequenceId) {
        Key key = new Key(new byte[] {(byte) runId});
        return new SSTMeta(
            runId,
            runId,
            runId,
            java.nio.file.Path.of("fake-" + runId + ".sst"),
            1,
            1,
            key,
            key,
            sequenceId,
            sequenceId,
            1,
            2,
            SSTState.NEW
        );
    }

    private static Table unsupportedMergeEngineTable() {
        return (Table) Proxy.newProxyInstance(
            PaimonSinkManagerIntegrationTest.class.getClassLoader(),
            new Class<?>[] {FileStoreTable.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "primaryKeys" -> List.of("id");
                case "partitionKeys" -> List.of();
                case "bucketMode" -> BucketMode.HASH_FIXED;
                case "coreOptions" -> new CoreOptions(Map.of("merge-engine", "partial-update"));
                case "toString" -> "partial-update-table";
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
    }

    private static Table crossPartitionTable() {
        return (Table) Proxy.newProxyInstance(
            PaimonSinkManagerIntegrationTest.class.getClassLoader(),
            new Class<?>[] {FileStoreTable.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "primaryKeys" -> List.of("id");
                case "partitionKeys" -> List.of("day");
                case "bucketMode" -> BucketMode.HASH_FIXED;
                case "coreOptions" -> new CoreOptions(Map.of("merge-engine", "deduplicate"));
                case "toString" -> "cross-partition-table";
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
    }

    private static Table unsupportedBucketModeTable(BucketMode bucketMode) {
        return (Table) Proxy.newProxyInstance(
            PaimonSinkManagerIntegrationTest.class.getClassLoader(),
            new Class<?>[] {FileStoreTable.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "primaryKeys" -> List.of("id");
                case "partitionKeys" -> List.of();
                case "bucketMode" -> bucketMode;
                case "coreOptions" -> new CoreOptions(Map.of("merge-engine", "deduplicate"));
                case "toString" -> "unsupported-bucket-mode-table";
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
    }

    private record TestEntry(Key key, Value value) {}

    private record TestTable(Catalog catalog, Table table) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }

    private static final class TrackingIterator implements SSTEntryIterator {
        private boolean closed;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public org.qwh.pms.core.memtable.model.Entry next() {
            throw new IllegalStateException("no entries");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class FailingSnapshotStorage implements LocalStorageManager {
        private final TrackingIterator firstIterator;
        private final List<SSTMeta> opened = new ArrayList<>();
        private boolean snapshotClosed;

        private FailingSnapshotStorage(TrackingIterator firstIterator) {
            this.firstIterator = firstIterator;
        }

        @Override
        public SSTMeta flushToSST(ImmutableMemTable memTable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSTReadSnapshot readSnapshot(List<SSTMeta> metas) {
            return new SSTReadSnapshot() {
                @Override
                public List<SSTMeta> metas() {
                    return metas;
                }

                @Override
                public Optional<Value> get(SSTMeta meta, Key key) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SSTEntryIterator openIterator(SSTMeta meta) {
                    opened.add(meta);
                    if (opened.size() == 1) {
                        return firstIterator;
                    }
                    throw new IllegalStateException("open failed for " + meta.runId());
                }

                @Override
                public SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive) {
                    return openIterator(meta);
                }

                @Override
                public void close() {
                    snapshotClosed = true;
                }
            };
        }

        @Override
        public SSTReadSnapshot readVisibleSnapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSTMeta compactSSTs(List<SSTMeta> metas) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSST(SSTMeta meta) {
            throw new UnsupportedOperationException();
        }

    }
}
