package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;
import org.qwh.pms.lookup.PaimonKeyValueLookupService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PaimonLocalTableQueryComparisonTest {

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
    void directLookupMatchesPaimonLocalTableQueryForLatestState() throws Exception {
        try (PaimonTableFixture fixture =
                        PaimonTableFixture.createPrimaryKeyTable(
                                "orders-local-query-comparison",
                                ROW_TYPE,
                                List.of("tenant_id", "order_id", "biz_date"));
                IOManager ioManager = localQueryIoManager();
                LocalTableQuery localQuery =
                        fixture.table().newLocalTableQuery().withIOManager(ioManager)) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService directLookup = service(fixture, index);

            install(directLookup, localQuery, fixture.writeRows(rows(100)));
            apply(
                    directLookup,
                    localQuery,
                    fixture.writeRows(List.of(row(42, 9_999), row(77, 7_777))));
            apply(directLookup, localQuery, fixture.writeRows(List.of(deleteRow(43))));

            assertHitMatchesLocalQuery(directLookup, localQuery, 42, 9_999);
            assertHitMatchesLocalQuery(directLookup, localQuery, 77, 7_777);
            assertHitMatchesLocalQuery(directLookup, localQuery, 41, 410);
            assertDeletedMatchesLocalQuery(directLookup, localQuery, 43);
            assertMissMatchesLocalQuery(directLookup, localQuery, 142);
        }
    }

    private static void assertHitMatchesLocalQuery(
            PaimonKeyValueLookupService directLookup,
            LocalTableQuery localQuery,
            int id,
            int expectedAmount)
            throws Exception {
        LookupResult direct = directLookup.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(id)));
        InternalRow local = localQuery.lookup(PARTITION, BUCKET, key(id));

        assertEquals(LookupResult.Kind.HIT, direct.kind());
        assertEquals(expectedAmount, direct.row().orElseThrow().getInt(3));
        assertEquals(expectedAmount, local.getInt(3));
    }

    private static void assertDeletedMatchesLocalQuery(
            PaimonKeyValueLookupService directLookup, LocalTableQuery localQuery, int id)
            throws Exception {
        LookupResult direct = directLookup.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(id)));
        InternalRow local = localQuery.lookup(PARTITION, BUCKET, key(id));

        assertEquals(LookupResult.Kind.DELETED, direct.kind());
        assertNull(local);
    }

    private static void assertMissMatchesLocalQuery(
            PaimonKeyValueLookupService directLookup, LocalTableQuery localQuery, int id)
            throws Exception {
        LookupResult direct = directLookup.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(id)));
        InternalRow local = localQuery.lookup(PARTITION, BUCKET, key(id));

        assertEquals(LookupResult.Kind.MISS, direct.kind());
        assertNull(local);
    }

    private static PaimonKeyValueLookupService service(
            PaimonTableFixture fixture, LiveFileIndex index) {
        return new PaimonKeyValueLookupService(
                index,
                new CandidatePlanner(FILE_KEY_COMPARATOR, 0),
                new PaimonKeyValueParquetLookup(
                        ROW_TYPE, KEY_FIELDS, fixture.schemaId(), fixture.dataFileResolver()),
                fixture.schemaId());
    }

    private static void apply(
            PaimonKeyValueLookupService directLookup,
            LocalTableQuery localQuery,
            PaimonTableFixture.WriteResult delta) {
        directLookup.applyDelta(PARTITION, BUCKET, delta.removedFiles(), delta.addedFiles());
        localQuery.refreshFiles(PARTITION, BUCKET, delta.removedFiles(), delta.addedFiles());
    }

    private static void install(
            PaimonKeyValueLookupService directLookup,
            LocalTableQuery localQuery,
            PaimonTableFixture.WriteResult snapshot) {
        directLookup.installSnapshot(PARTITION, BUCKET, snapshot.addedFiles());
        localQuery.refreshFiles(PARTITION, BUCKET, List.of(), snapshot.addedFiles());
    }

    private static IOManager localQueryIoManager() throws Exception {
        Path path = Path.of("target", "paimon-local-query").toAbsolutePath();
        Files.createDirectories(path);
        return IOManager.create(path.toString());
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
