package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

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

    private static InternalRow row(int id, int amount) {
        return GenericRow.of(id / 100, (long) id, amount);
    }
}
