package org.qwh.pms.lookup;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.parquet.ParquetKeyRowLocator;
import org.qwh.pms.lookup.parquet.ParquetLookupMetadataCache;
import org.qwh.pms.lookup.parquet.ResolvedDataFileKey;
import org.qwh.pms.lookup.parquet.RowRangeCandidate;
import org.qwh.pms.lookup.parquet.key.LookupKeySpec;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetKeyRowLocatorTest {
    @Test
    void stopsOnGreaterKeyAndPreservesPositionsAcrossBatchesAndRanges() throws Exception {
        RowType rowType = RowType.builder().field("id", DataTypes.INT().notNull()).build();
        try (var fixture = PaimonTableFixture.createPrimaryKeyTable("ordered-key-locator", rowType, List.of("id"))) {
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 512; i++) rows.add(GenericRow.of(i * 2));
            var file = fixture.writeRows(rows).newFiles().get(0);
            var resolved = fixture.dataFileResolver().resolve(new FileLookupContext(BinaryRow.EMPTY_ROW, 0), file);
            var metadata = new ParquetLookupMetadataCache(new Options())
                    .getOrLoad(ResolvedDataFileKey.of(file, resolved), resolved);
            var keyType = PrimaryKeyTableUtils.addKeyNamePrefix(rowType);
            var locator = new ParquetKeyRowLocator(
                    LookupKeySpec.of(KeyValue.schema(keyType, rowType), 0), new Options(), 8);
            var range = new RowRangeCandidate(0, 512);

            // Count target-key accesses to detect a full-range scan, even when it returns
            // the correct MISS. This requires no production instrumentation or reader stub.
            for (int missing : new int[]{-1, 3, 15}) {
                AtomicInteger comparisons = new AtomicInteger();
                InternalRow key = countingKey(missing, comparisons);
                assertEquals(-1, locator.locate(metadata, range, key));
                assertTrue(comparisons.get() > 0 && comparisons.get() <= 10,
                        "MISS should stop near the target, not compare all 512 rows");
            }
            for (int index : new int[]{0, 7, 8, 128, 511}) {
                assertEquals(index, locator.locate(metadata, range, GenericRow.of(index * 2)));
            }
            assertEquals(-1, locator.locate(metadata, range, GenericRow.of(1023)));
            var subRange = new RowRangeCandidate(128, 256);
            assertEquals(129, locator.locate(metadata, subRange, GenericRow.of(258)));
            assertEquals(-1, locator.locate(metadata, subRange, GenericRow.of(255)));
        }
    }

    private static InternalRow countingKey(int value, AtomicInteger comparisons) {
        InternalRow delegate = GenericRow.of(value);
        return (InternalRow) Proxy.newProxyInstance(InternalRow.class.getClassLoader(),
                new Class<?>[]{InternalRow.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getInt")) comparisons.incrementAndGet();
                    return method.invoke(delegate, args);
                });
    }
}
