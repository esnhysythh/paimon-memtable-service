package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.DataFileResolver;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.ResolvedDataFile;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonTableFixtureTest {

    private static final FileLookupContext CONTEXT =
            new FileLookupContext(BinaryRow.EMPTY_ROW, 0);
    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("tenant_id", DataTypes.INT())
                    .field("order_id", DataTypes.BIGINT())
                    .field("biz_date", DataTypes.DATE())
                    .field("amount", DataTypes.INT())
                    .build();

    @Test
    void createsRealPrimaryKeyTableAndExposesDataFileDelta() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            PaimonTableFixture.WriteResult result = fixture.writeRows(rows(1_000));

            assertFalse(result.commitMessages().isEmpty());
            assertFalse(result.newFiles().isEmpty());
            assertTrue(result.deletedFiles().isEmpty());
            assertTrue(result.compactBefore().isEmpty());
            assertTrue(result.compactAfter().isEmpty());
            assertEquals(
                    List.of("tenant_id", "order_id", "biz_date"),
                    fixture.table().schema().primaryKeys());
            assertTrue(fixture.tablePath().toString().contains("target/paimon-fixtures/orders-"));

            DataFileResolver resolver = fixture.dataFileResolver();
            for (DataFileMeta file : result.newFiles()) {
                ResolvedDataFile resolved = resolver.resolve(CONTEXT, file);
                assertEquals(file.fileSize(), resolved.fileSize());
                assertTrue(resolved.fileIO().exists(resolved.path()));
                assertTrue(resolved.path().getName().endsWith(".parquet"));
            }
        }
    }

    @Test
    void keyValueLookupReadsInsertAndDeleteMarkersFromRealPaimonFiles() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-kv", ROW_TYPE, List.of("tenant_id", "order_id", "biz_date"))) {
            PaimonTableFixture.WriteResult insertResult = fixture.writeRows(rows(100));
            PaimonTableFixture.WriteResult deleteResult =
                    fixture.writeRows(List.of(deleteRow(42)));

            PaimonKeyValueParquetLookup lookup =
                    new PaimonKeyValueParquetLookup(
                            ROW_TYPE,
                            new int[] {0, 1, 2},
                            fixture.schemaId(),
                            fixture.dataFileResolver());

            LookupResult insertHit =
                    lookup.lookup(
                            CONTEXT,
                            insertResult.newFiles().get(0),
                            LookupRequest.projected(key(42), new int[] {3}));
            assertEquals(LookupResult.Kind.HIT, insertHit.kind());
            assertEquals(420, insertHit.row().orElseThrow().getInt(0));
            assertEquals(42L, insertHit.rowIndex());

            LookupResult deleteHit =
                    lookup.lookup(
                            CONTEXT,
                            deleteResult.newFiles().get(0), LookupRequest.fullRow(key(42)));
            assertEquals(LookupResult.Kind.DELETED, deleteHit.kind());
            assertEquals(0L, deleteHit.rowIndex());

            LookupResult miss =
                    lookup.lookup(
                            CONTEXT,
                            deleteResult.newFiles().get(0), LookupRequest.fullRow(key(43)));
            assertEquals(LookupResult.Kind.MISS, miss.kind());
        }
    }

    private static List<InternalRow> rows(int rowCount) {
        List<InternalRow> rows = new ArrayList<>(rowCount);
        for (int id = 0; id < rowCount; id++) {
            rows.add(GenericRow.of(id / 100, (long) id, 20_000 + (id % 30), id * 10));
        }
        return rows;
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
