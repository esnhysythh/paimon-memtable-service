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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.local.LocalCacheBuildContext;
import org.qwh.pms.lookup.local.LocalCacheDirectory;
import org.qwh.pms.lookup.local.LocalCacheEntry;
import org.qwh.pms.lookup.local.LocalCacheMode;
import org.qwh.pms.lookup.local.ValueSstCacheBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueSstCacheTest {

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

    @Test
    void valueSstReadsInsertDeleteAndMissFromRealPaimonFiles() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-value-sst", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createDirectories(Path.of("target", "value-sst-cache"));
            try (LocalCacheDirectory directory = new LocalCacheDirectory(cacheDir)) {
                ValueSstCacheBuilder builder = valueSstBuilder(fixture, directory);
                LocalCacheBuildContext context = new LocalCacheBuildContext(PARTITION, BUCKET);
                PaimonTableFixture.WriteResult insertResult = fixture.writeRows(rows(100));
                PaimonTableFixture.WriteResult deleteResult =
                        fixture.writeRows(List.of(deleteRow(42)));

                DataFileMeta insertFile = insertResult.newFiles().get(0);
                Path insertCacheFile = directory.cacheFile(insertFile, context).toPath();
                try (LocalCacheEntry insertEntry = builder.build(insertFile, context)) {
                    assertEquals(LocalCacheMode.VALUE_SST, insertEntry.mode());
                    assertTrue(Files.exists(insertCacheFile));

                    LookupResult hit =
                            insertEntry.lookup(LookupRequest.projected(key(42), new int[] {3}));
                    assertEquals(LookupResult.Kind.HIT, hit.kind());
                    assertEquals(420, hit.row().orElseThrow().getInt(0));

                    LookupResult miss = insertEntry.lookup(LookupRequest.fullRow(key(142)));
                    assertEquals(LookupResult.Kind.MISS, miss.kind());
                    try (java.util.stream.Stream<Path> files = Files.list(cacheDir)) {
                        assertEquals(2, files.count());
                    }
                }
                assertFalse(Files.exists(insertCacheFile));

                DataFileMeta deleteFile = deleteResult.newFiles().get(0);
                try (LocalCacheEntry deleteEntry = builder.build(deleteFile, context)) {
                    LookupResult deleted = deleteEntry.lookup(LookupRequest.fullRow(key(42)));
                    assertEquals(LookupResult.Kind.DELETED, deleted.kind());
                }
            }
        }
    }

    @Test
    void localCacheDirectoryCleansFilesLeftByPreviousProcess() throws Exception {
        Path cacheDir = Files.createTempDirectory("value-sst-orphan-");
        Files.writeString(cacheDir.resolve("old.value-sst"), "orphan");
        Files.writeString(cacheDir.resolve("old.value-sst.building-123"), "unfinished");

        try (LocalCacheDirectory directory = new LocalCacheDirectory(cacheDir)) {
            try (java.util.stream.Stream<Path> files = Files.list(cacheDir)) {
                assertEquals(1, files.count());
            }
        }
        assertTrue(Files.isDirectory(cacheDir));
    }

    @Test
    void localCacheDirectoryRejectsAnotherOwnerForTheSameRoot() throws Exception {
        Path cacheDir = Files.createTempDirectory("value-sst-owner-");
        try (LocalCacheDirectory first = new LocalCacheDirectory(cacheDir)) {
            assertThrows(IOException.class, () -> new LocalCacheDirectory(cacheDir));
        }
    }

    @Test
    void valueSstBuildDoesNotDeleteFinalFilePublishedByAnotherBuilder() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-value-sst-publish-race",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            Path cacheDir = Files.createTempDirectory("value-sst-publish-race-");
            PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(10));
            DataFileMeta file = writeResult.newFiles().get(0);
            Path cacheFile = cachePath(cacheDir, file);
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
            ValueSstCacheBuilder builder =
                    new ValueSstCacheBuilder(
                            FILE_KEY_TYPE,
                            ROW_TYPE,
                            (ignored, context) -> {
                                Files.writeString(cacheFile, "published-by-another-builder");
                                return readerFactory.createRecordReader(file);
                            },
                            lookupStoreFactory,
                            LookupSerializerFactory.INSTANCE.get(),
                            LookupStoreFactory.bfGenerator(options.toConfiguration()),
                            (ignored, context) -> cacheFile.toFile());

            assertThrows(
                    java.io.IOException.class,
                    () -> builder.build(file, new LocalCacheBuildContext(PARTITION, BUCKET)));
            assertEquals("published-by-another-builder", Files.readString(cacheFile));
        }
    }

    private static ValueSstCacheBuilder valueSstBuilder(
            PaimonTableFixture fixture, LocalCacheDirectory directory) {
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
                directory);
    }

    private static Path cachePath(Path cacheDir, DataFileMeta file) {
        return cacheDir.resolve(file.fileName() + ".value-sst");
    }

    private static List<InternalRow> rows(int rowCount) {
        return java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(id -> row(id, id * 10))
                .map(InternalRow.class::cast)
                .toList();
    }

    private static GenericRow row(int id, int amount) {
        return GenericRow.of(id / 100, (long) id, 20_000 + (id % 30), amount);
    }

    private static GenericRow deleteRow(int id) {
        return GenericRow.ofKind(RowKind.DELETE, id / 100, (long) id, 20_000 + (id % 30), id * 10);
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
}
