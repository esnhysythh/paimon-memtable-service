package org.qwh.pms.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;
import org.qwh.pms.lookup.cache.valuesst.ValueSstCacheBuilder;
import org.qwh.pms.lookup.PaimonKeyValueLookupService;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ThresholdFileLookupRouterTest {

    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;
    private static final int BUCKET = 0;
    private static final int[] KEY_FIELDS = {0, 1, 2};
    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("tenant_id", DataTypes.INT())
                    .field("order_id", DataTypes.BIGINT())
                    .field("biz_date", DataTypes.DATE())
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType FILE_KEY_TYPE =
            PrimaryKeyTableUtils.addKeyNamePrefix(ROW_TYPE.project(KEY_FIELDS));
    private static final Comparator<InternalRow> FILE_KEY_COMPARATOR =
            new KeyComparatorSupplier(FILE_KEY_TYPE).get();

    @Test
    void routesThroughDirectLookupUntilAsynchronousBuildIsReady() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-threshold-router", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createDirectories(Path.of("target", "threshold-router-cache"));
            ManualExecutor executor = new ManualExecutor();
            CountingDataFileLookup directLookup =
                    new CountingDataFileLookup(
                            new PaimonKeyValueParquetLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()));

            try (ThresholdFileLookupRouter router =
                    new ThresholdFileLookupRouter(
                            directLookup, valueSstBuilder(fixture, cacheDir), executor)) {
                PaimonKeyValueLookupService service = service(router, fixture.schemaId());
                PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(100));
                service.installSnapshot(PARTITION, BUCKET, writeResult.addedFiles());

                DataFileMeta file = writeResult.newFiles().get(0);
                Path cacheFile = cachePath(cacheDir, file);
                assertHit(service, 42);
                assertHit(service, 42);
                assertHit(service, 42);
                assertEquals(3, directLookup.lookupCount);
                assertEquals(1, executor.pendingTaskCount());
                assertFalse(Files.exists(cacheFile));

                assertHit(service, 42);
                assertEquals(4, directLookup.lookupCount);

                executor.runAll();
                assertTrue(Files.exists(cacheFile));

                assertHit(service, 42);
                assertEquals(4, directLookup.lookupCount);

                service.applyDelta(PARTITION, BUCKET, List.of(file), List.of());
                assertFalse(Files.exists(cacheFile));
                assertEquals(
                        LookupResult.Kind.MISS,
                        service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42))).kind());
            }
        }
    }

    @Test
    void removesReadyCacheAndDiscardsQueuedBuildWhenFileLeavesLiveView() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-threshold-invalidate", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createDirectories(Path.of("target", "threshold-router-cache"));
            ManualExecutor executor = new ManualExecutor();
            CountingDataFileLookup directLookup =
                    new CountingDataFileLookup(
                            new PaimonKeyValueParquetLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()));

            try (ThresholdFileLookupRouter router =
                    new ThresholdFileLookupRouter(
                            directLookup, valueSstBuilder(fixture, cacheDir), executor)) {
                PaimonKeyValueLookupService service = service(router, fixture.schemaId());
                PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(100));
                service.installSnapshot(PARTITION, BUCKET, writeResult.addedFiles());

                DataFileMeta file = writeResult.newFiles().get(0);
                Path cacheFile = cachePath(cacheDir, file);
                assertHit(service, 42);
                assertHit(service, 42);
                assertHit(service, 42);
                assertEquals(1, executor.pendingTaskCount());

                service.applyDelta(PARTITION, BUCKET, List.of(file), List.of());
                executor.runAll();

                assertFalse(Files.exists(cacheFile));
                assertEquals(
                        LookupResult.Kind.MISS,
                        service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42))).kind());
            }
        }
    }

    @Test
    void replacingBucketSnapshotDiscardsExistingLocalEntries() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-threshold-snapshot", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createDirectories(Path.of("target", "threshold-router-cache"));
            ManualExecutor executor = new ManualExecutor();
            CountingDataFileLookup directLookup =
                    new CountingDataFileLookup(
                            new PaimonKeyValueParquetLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()));

            try (ThresholdFileLookupRouter router =
                    new ThresholdFileLookupRouter(
                            directLookup, valueSstBuilder(fixture, cacheDir), executor)) {
                PaimonKeyValueLookupService service = service(router, fixture.schemaId());
                PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(100));
                service.installSnapshot(PARTITION, BUCKET, writeResult.addedFiles());
                DataFileMeta file = writeResult.newFiles().get(0);
                Path cacheFile = cachePath(cacheDir, file);

                assertHit(service, 42);
                assertHit(service, 42);
                assertHit(service, 42);
                executor.runAll();
                assertTrue(Files.exists(cacheFile));

                service.installSnapshot(PARTITION, BUCKET, writeResult.addedFiles());
                assertFalse(Files.exists(cacheFile));
                assertEquals(0, router.stats().readyEntries());
            }
        }
    }

    @Test
    void schemaMismatchNeverAdmitsOrBuildsLocalCache() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-threshold-schema-mismatch",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createTempDirectory("threshold-schema-mismatch-");
            ManualExecutor executor = new ManualExecutor();
            CountingDataFileLookup directLookup =
                    new CountingDataFileLookup(
                            new PaimonKeyValueParquetLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()));

            try (ThresholdFileLookupRouter router =
                    new ThresholdFileLookupRouter(
                            directLookup, valueSstBuilder(fixture, cacheDir), executor, 1)) {
                PaimonKeyValueLookupService service = service(router, fixture.schemaId() + 1);
                PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(100));
                service.installSnapshot(PARTITION, BUCKET, writeResult.addedFiles());

                assertThrows(
                        SchemaMismatchException.class,
                        () -> service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42))));
                assertThrows(
                        SchemaMismatchException.class,
                        () -> service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42))));
                assertEquals(0, directLookup.lookupCount);
                assertEquals(0, executor.pendingTaskCount());
                assertEquals(0, router.stats().buildsScheduled());
                assertEquals(0, router.stats().readyEntries());
            }
        }
    }

    @Test
    void realCompactionDeltaInvalidatesOldLocalCacheAndBuildsForCompactedFile() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-threshold-compaction",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createTempDirectory("threshold-compaction-cache-");
            ManualExecutor executor = new ManualExecutor();
            CountingDataFileLookup directLookup =
                    new CountingDataFileLookup(
                            new PaimonKeyValueParquetLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()));

            try (ThresholdFileLookupRouter router =
                    new ThresholdFileLookupRouter(
                            directLookup, valueSstBuilder(fixture, cacheDir), executor, 1)) {
                PaimonKeyValueLookupService service = service(router, fixture.schemaId());
                PaimonTableFixture.WriteResult initialWrite = fixture.writeRows(rows(100));
                service.installSnapshot(PARTITION, BUCKET, initialWrite.addedFiles());
                DataFileMeta initialFile = initialWrite.newFiles().get(0);
                Path initialCacheFile = cachePath(cacheDir, initialFile);

                assertHit(service, 42);
                executor.runAll();
                assertHit(service, 42);
                assertTrue(Files.exists(initialCacheFile));

                PaimonTableFixture.WriteResult update =
                        fixture.writeRows(List.of(GenericRow.of(0, 42L, 20_012, 9_999)));
                service.applyDelta(PARTITION, BUCKET, update.removedFiles(), update.addedFiles());

                PaimonTableFixture.WriteResult compaction = fixture.compact(PARTITION, BUCKET);
                assertTrue(compaction.compactBefore().contains(initialFile));
                assertFalse(compaction.compactAfter().isEmpty());

                service.applyDelta(
                        PARTITION, BUCKET, compaction.removedFiles(), compaction.addedFiles());
                assertFalse(Files.exists(initialCacheFile));
                assertTrue(
                        directLookup.invalidatedFiles.containsAll(
                                compaction.compactBefore().stream()
                                        .map(DataFileMeta::fileName)
                                        .collect(java.util.stream.Collectors.toSet())));

                assertUpdatedHit(service);
                executor.runAll();
                assertUpdatedHit(service);
                assertEquals(1, router.stats().readyEntries());
            }
        }
    }

    private static PaimonKeyValueLookupService service(
            DataFileLookup fileLookup, long expectedSchemaId) {
        return new PaimonKeyValueLookupService(
                new LiveFileIndex(FILE_KEY_COMPARATOR, 4),
                new CandidatePlanner(FILE_KEY_COMPARATOR, 0),
                fileLookup,
                expectedSchemaId);
    }

    private static ValueSstCacheBuilder valueSstBuilder(
            PaimonTableFixture fixture, Path cacheDir) {
        CoreOptions options = fixture.table().coreOptions();
        LookupStoreFactory lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(FILE_KEY_TYPE).createSliceComparator());
        KeyValueFileReaderFactory readerFactory =
                ((KeyValueFileStore) fixture.table().store())
                        .newReaderFactoryBuilder()
                        .build(PARTITION, BUCKET, DeletionVector.emptyFactory());

        return new ValueSstCacheBuilder(
                FILE_KEY_TYPE,
                ROW_TYPE,
                (file, context) -> {
                    assertEquals(PARTITION, context.partition());
                    assertEquals(BUCKET, context.bucket());
                    return readerFactory.createRecordReader(file);
                },
                lookupStoreFactory,
                LookupSerializerFactory.INSTANCE.get(),
                LookupStoreFactory.bfGenerator(options.toConfiguration()),
                (file, ignored) -> cachePath(cacheDir, file).toFile());
    }

    private static void assertHit(PaimonKeyValueLookupService service, int id) throws Exception {
        LookupResult result = service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(id)));
        assertEquals(LookupResult.Kind.HIT, result.kind());
        assertEquals(id * 10, result.row().orElseThrow().getInt(3));
    }

    private static void assertUpdatedHit(PaimonKeyValueLookupService service) throws Exception {
        LookupResult result = service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42)));
        assertEquals(LookupResult.Kind.HIT, result.kind());
        assertEquals(9_999, result.row().orElseThrow().getInt(3));
    }

    private static Path cachePath(Path cacheDir, DataFileMeta file) {
        return cacheDir.resolve(file.fileName() + ".value-sst");
    }

    private static List<InternalRow> rows(int rowCount) {
        return java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(id -> GenericRow.of(id / 100, (long) id, 20_000 + (id % 30), id * 10))
                .map(InternalRow.class::cast)
                .toList();
    }

    private static BinaryRow key(int id) {
        BinaryRow row = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, id / 100);
        writer.writeLong(1, id);
        writer.writeInt(2, 20_000 + (id % 30));
        writer.complete();
        return row;
    }

    private static final class CountingDataFileLookup implements DataFileLookup {

        private final DataFileLookup delegate;
        private int lookupCount;
        private final Set<String> invalidatedFiles = new HashSet<>();

        private CountingDataFileLookup(DataFileLookup delegate) {
            this.delegate = delegate;
        }

        @Override
        public LookupResult lookup(
                FileLookupContext context, DataFileMeta file, LookupRequest request)
                throws java.io.IOException {
            lookupCount++;
            return delegate.lookup(context, file, request);
        }

        @Override
        public void invalidate(FileLookupContext context, DataFileMeta file) {
            invalidatedFiles.add(file.fileName());
            delegate.invalidate(context, file);
        }

        @Override
        public void invalidateBucket(FileLookupContext context) {
            delegate.invalidateBucket(context);
        }
    }

    private static final class ManualExecutor implements Executor {

        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int pendingTaskCount() {
            return tasks.size();
        }

        private void runAll() {
            while (!tasks.isEmpty()) {
                tasks.remove().run();
            }
        }
    }
}
