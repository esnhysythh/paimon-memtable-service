package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.parquet.ParquetLookupMetadata;
import org.qwh.pms.lookup.parquet.ParquetLookupMetadataCache;
import org.qwh.pms.lookup.parquet.ResolvedDataFileKey;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetLookupMetadataCacheTest {

    private static final FileLookupContext CONTEXT =
            new FileLookupContext(BinaryRow.EMPTY_ROW, 0);
    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("tenant_id", DataTypes.INT())
                    .field("order_id", DataTypes.BIGINT())
                    .field("amount", DataTypes.INT())
                    .build();

    @Test
    void cachesMetadataByResolvedFileIdentityAndInvalidatesByFileName() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-metadata-cache", ROW_TYPE, List.of("tenant_id", "order_id"))) {
            PaimonTableFixture.WriteResult writeResult =
                    fixture.writeRows(List.of(row(1, 100), row(2, 200)));
            DataFileMeta file = writeResult.newFiles().get(0);
            ResolvedDataFile resolved = fixture.dataFileResolver().resolve(CONTEXT, file);
            ResolvedDataFileKey key = ResolvedDataFileKey.of(file, resolved);
            ParquetLookupMetadataCache cache = new ParquetLookupMetadataCache(new Options());

            ParquetLookupMetadata first = cache.getOrLoad(key, resolved);
            ParquetLookupMetadata second = cache.getOrLoad(key, resolved);
            assertSame(first, second);

            cache.invalidate(file.fileName());

            ParquetLookupMetadata reloaded = cache.getOrLoad(key, resolved);
            assertNotSame(first, reloaded);
        }
    }

    @Test
    void evictsLeastRecentlyUsedMetadataWhenCapacityIsReached() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-metadata-cache-capacity",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id"))) {
            DataFileMeta firstFile = fixture.writeRows(List.of(row(1, 100))).newFiles().get(0);
            DataFileMeta secondFile = fixture.writeRows(List.of(row(2, 200))).newFiles().get(0);
            DataFileMeta thirdFile = fixture.writeRows(List.of(row(3, 300))).newFiles().get(0);
            ResolvedDataFile first = fixture.dataFileResolver().resolve(CONTEXT, firstFile);
            ResolvedDataFile second = fixture.dataFileResolver().resolve(CONTEXT, secondFile);
            ResolvedDataFile third = fixture.dataFileResolver().resolve(CONTEXT, thirdFile);
            ResolvedDataFileKey firstKey = ResolvedDataFileKey.of(firstFile, first);
            ResolvedDataFileKey secondKey = ResolvedDataFileKey.of(secondFile, second);
            ResolvedDataFileKey thirdKey = ResolvedDataFileKey.of(thirdFile, third);
            ParquetLookupMetadataCache cache = new ParquetLookupMetadataCache(new Options(), 2);

            ParquetLookupMetadata firstLoaded = cache.getOrLoad(firstKey, first);
            ParquetLookupMetadata secondLoaded = cache.getOrLoad(secondKey, second);
            assertSame(firstLoaded, cache.getOrLoad(firstKey, first));
            cache.getOrLoad(thirdKey, third);

            assertEquals(2, cache.size());
            assertSame(firstLoaded, cache.getOrLoad(firstKey, first));
            assertNotSame(secondLoaded, cache.getOrLoad(secondKey, second));
            assertEquals(2, cache.size());

            cache.clear();
            assertEquals(0, cache.size());
        }
    }

    @Test
    void invalidationDuringLoadDoesNotRepopulateCache() throws Exception {
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable(
                "metadata-invalidation-race", ROW_TYPE, List.of("tenant_id", "order_id"))) {
            var file = fixture.writeRows(List.of(row(1, 100))).newFiles().get(0);
            var resolved = fixture.dataFileResolver().resolve(CONTEXT, file);
            var key = ResolvedDataFileKey.of(file, resolved);
            for (boolean clearAll : new boolean[]{false, true}) {
                var started = new CountDownLatch(1);
                var resume = new CountDownLatch(1);
                var blockOnce = new AtomicBoolean(true);
                var io = new LocalFileIO() {
                    @Override
                    public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
                        if (blockOnce.compareAndSet(true, false)) {
                            started.countDown();
                            try {
                                if (!resume.await(10, TimeUnit.SECONDS)) throw new IOException("load timed out");
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException(e);
                            }
                        }
                        return super.newInputStream(path);
                    }
                };
                var tracked = new ResolvedDataFile(io, resolved.path(), resolved.fileSize());
                var cache = new ParquetLookupMetadataCache(new Options());
                var executor = Executors.newSingleThreadExecutor();
                try {
                    var loading = executor.submit(() -> cache.getOrLoad(key, tracked));
                    assertTrue(started.await(5, TimeUnit.SECONDS));
                    if (clearAll) cache.clear(); else cache.invalidate(file.fileName());
                    resume.countDown();
                    var old = loading.get(10, TimeUnit.SECONDS);
                    assertEquals(0, cache.size(), "invalidated in-flight load must not repopulate cache");
                    var fresh = cache.getOrLoad(key, tracked);
                    assertNotSame(old, fresh);
                    assertSame(fresh, cache.getOrLoad(key, tracked));
                } finally {
                    resume.countDown();
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }

    private static InternalRow row(int id, int amount) {
        return GenericRow.of(id / 100, (long) id, amount);
    }
}
