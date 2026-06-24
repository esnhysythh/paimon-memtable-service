package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.api.SchemaMismatchException;
import org.qwh.pms.lookup.direct.parquet.PaimonKeyValueDirectLookup;
import org.qwh.pms.lookup.live.CandidatePlanner;
import org.qwh.pms.lookup.live.LiveFileIndex;
import org.qwh.pms.lookup.paimon.PaimonKeyValueLookupService;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonKeyValueLookupServiceTest {

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
    private static final int[] STRING_KEY_FIELDS = {0};
    private static final RowType STRING_ROW_TYPE =
            RowType.builder()
                    .field("external_id", DataTypes.STRING())
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType STRING_FILE_KEY_TYPE =
            PrimaryKeyTableUtils.addKeyNamePrefix(STRING_ROW_TYPE.project(STRING_KEY_FIELDS));
    private static final Comparator<InternalRow> STRING_FILE_KEY_COMPARATOR =
            new KeyComparatorSupplier(STRING_FILE_KEY_TYPE).get();
    private static final RowType BINARY_ROW_TYPE =
            RowType.builder()
                    .field("external_id", DataTypes.BYTES())
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType TIMESTAMP_NANOS_ROW_TYPE =
            RowType.builder()
                    .field("event_time", DataTypes.TIMESTAMP(9))
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType TIMESTAMP_MILLIS_ROW_TYPE =
            RowType.builder()
                    .field("event_time", DataTypes.TIMESTAMP(3))
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType TIMESTAMP_MICROS_ROW_TYPE =
            RowType.builder()
                    .field("event_time", DataTypes.TIMESTAMP(6))
                    .field("amount", DataTypes.INT())
                    .build();
    private static final RowType TIMESTAMP_LTZ_ROW_TYPE =
            RowType.builder()
                    .field("event_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                    .field("amount", DataTypes.INT())
                    .build();

    @Test
    void lookupReturnsLatestValueAcrossOverlappingFiles() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-update",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service = service(fixture, index);

            install(service, fixture.writeRows(rows(100)));
            apply(service, fixture.writeRows(List.of(row(42, 9_999))));

            LookupResult latest =
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.projected(key(42), new int[] {3}));
            assertEquals(LookupResult.Kind.HIT, latest.kind());
            assertEquals(9_999, latest.row().orElseThrow().getInt(0));

            LookupResult oldOnlyKey =
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.projected(key(41), new int[] {3}));
            assertEquals(LookupResult.Kind.HIT, oldOnlyKey.kind());
            assertEquals(410, oldOnlyKey.row().orElseThrow().getInt(0));
        }
    }

    @Test
    void committedMessagesUpdateAnInitializedBucketView() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-committed-message",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            PaimonKeyValueLookupService service =
                    service(fixture, new LiveFileIndex(FILE_KEY_COMPARATOR, 4));
            service.installSnapshot(PARTITION, BUCKET, List.of());

            PaimonTableFixture.WriteResult committed = fixture.writeRows(List.of(row(42, 9_999)));
            service.applyCommittedMessages(committed.commitMessages());

            LookupResult result =
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42)));
            assertEquals(LookupResult.Kind.HIT, result.kind());
            assertEquals(9_999, result.row().orElseThrow().getInt(3));
        }
    }

    @Test
    void lookupShortCircuitsOnNewestDeleteMarker() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-delete",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service = service(fixture, index);

            install(service, fixture.writeRows(rows(100)));
            apply(service, fixture.writeRows(List.of(deleteRow(42))));

            LookupResult deleted =
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(42)));
            assertEquals(LookupResult.Kind.DELETED, deleted.kind());

            LookupResult miss =
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(142)));
            assertEquals(LookupResult.Kind.MISS, miss.kind());
        }
    }

    @Test
    void missingOrInvalidBucketReturnsUnknown() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-unknown",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service = service(fixture, index);

            assertEquals(
                    LookupResult.Kind.UNKNOWN,
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(1))).kind());

            install(service, fixture.writeRows(rows(10)));
            index.invalidate(PARTITION, BUCKET);

            assertEquals(
                    LookupResult.Kind.UNKNOWN,
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(1))).kind());
        }
    }

    @Test
    void lookupRejectsNullKey() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-null-key",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service = service(fixture, index);

            install(service, fixture.writeRows(rows(10)));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(nullKey())));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(null)));
        }
    }

    @Test
    void lookupReturnsUnknownWhenFileLookupFails() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-io-failure",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            LiveFileIndex index = new LiveFileIndex(FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service =
                    new PaimonKeyValueLookupService(
                            index,
                            new CandidatePlanner(FILE_KEY_COMPARATOR, 0),
                            new PaimonKeyValueDirectLookup(
                                    ROW_TYPE,
                                    KEY_FIELDS,
                                    fixture.schemaId(),
                                    (context, file) -> {
                                        throw new java.io.IOException("simulated read failure");
                                    }),
                            fixture.schemaId());

            install(service, fixture.writeRows(rows(10)));

            assertEquals(
                    LookupResult.Kind.UNKNOWN,
                    service.lookup(PARTITION, BUCKET, LookupRequest.fullRow(key(1))).kind());
        }
    }

    @Test
    void lookupFailsFastWhenFileSchemaDiffersFromConfiguredSchema() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-schema-mismatch",
                        ROW_TYPE,
                        List.of("tenant_id", "order_id", "biz_date"))) {
            PaimonTableFixture.WriteResult writeResult = fixture.writeRows(rows(10));
            PaimonKeyValueDirectLookup lookup =
                    new PaimonKeyValueDirectLookup(
                            ROW_TYPE,
                            KEY_FIELDS,
                            fixture.schemaId() + 1,
                            fixture.dataFileResolver());

            SchemaMismatchException exception =
                    assertThrows(
                            SchemaMismatchException.class,
                            () ->
                                    lookup.lookup(
                                            new FileLookupContext(PARTITION, BUCKET),
                                            writeResult.newFiles().get(0),
                                            LookupRequest.fullRow(key(1))));
            assertTrue(exception.getMessage().contains("expected schemaId="));
        }
    }

    @Test
    void lookupSupportsStringKeysInPaimonKeyValueFiles() throws Exception {
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-string",
                        STRING_ROW_TYPE,
                        List.of("external_id"))) {
            LiveFileIndex index = new LiveFileIndex(STRING_FILE_KEY_COMPARATOR, 4);
            PaimonKeyValueLookupService service =
                    new PaimonKeyValueLookupService(
                            index,
                            new CandidatePlanner(STRING_FILE_KEY_COMPARATOR, 0),
                            new PaimonKeyValueDirectLookup(
                                    STRING_ROW_TYPE,
                                    STRING_KEY_FIELDS,
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()),
                            fixture.schemaId());

            install(service, fixture.writeRows(stringRows(100)));
            apply(service, fixture.writeRows(List.of(stringRow(42, 9_999))));

            LookupResult latest =
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.projected(stringKey(42), new int[] {1}));
            assertEquals(LookupResult.Kind.HIT, latest.kind());
            assertEquals(9_999, latest.row().orElseThrow().getInt(0));

            LookupResult miss =
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.fullRow(stringKey(1_500)));
            assertEquals(LookupResult.Kind.MISS, miss.kind());
        }
    }

    @Test
    void lookupPreservesLongUnicodePrefixStringStatsBoundaries() throws Exception {
        // The UTF-8 prefix exceeds Parquet's default column-index truncation length.
        String commonPrefix = "订单-".repeat(48);
        String lower = commonPrefix + "0000";
        String middle = commonPrefix + "5000";
        String upper = commonPrefix + "zzzz";
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(
                        "orders-service-string-prefix", STRING_ROW_TYPE, List.of("external_id"))) {
            PaimonKeyValueLookupService service = stringService(fixture);
            install(
                    service,
                    fixture.writeRows(
                            List.of(
                                    GenericRow.of(BinaryString.fromString(lower), 100),
                                    GenericRow.of(BinaryString.fromString(middle), 200),
                                    GenericRow.of(BinaryString.fromString(upper), 300))));

            assertStringHit(service, lower, 100);
            assertStringHit(service, middle, 200);
            assertStringHit(service, upper, 300);
            assertEquals(
                    LookupResult.Kind.MISS,
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.fullRow(stringKey(commonPrefix + "7000")))
                            .kind());
        }
    }

    @Test
    void lookupSupportsTimestampMillisAndMicrosKeysInPaimonKeyValueFiles() throws Exception {
        assertTimestampLookup(
                "orders-service-timestamp-millis",
                TIMESTAMP_MILLIS_ROW_TYPE,
                3,
                Timestamp.fromEpochMillis(1_700_000_000_123L));
        assertTimestampLookup(
                "orders-service-timestamp-micros",
                TIMESTAMP_MICROS_ROW_TYPE,
                6,
                Timestamp.fromEpochMillis(1_700_000_000_123L, 456_000));
    }

    @Test
    void lookupRejectsBinaryKeysInPaimonKeyValueFiles() {
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        new PaimonKeyValueDirectLookup(
                                BINARY_ROW_TYPE, new int[] {0}, 0L, (context, ignored) -> null));
    }

    @Test
    void lookupRejectsTimestampInt96KeysInPaimonKeyValueFiles() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> new PaimonKeyValueDirectLookup(
                        TIMESTAMP_NANOS_ROW_TYPE,
                        new int[] {0},
                        0L,
                        (context, ignored) -> null));
    }

    @Test
    void lookupRejectsTimestampLtzKeysUntilTimezoneSemanticsAreVerified() {
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        new PaimonKeyValueDirectLookup(
                                TIMESTAMP_LTZ_ROW_TYPE,
                                new int[] {0},
                                0L,
                                (context, ignored) -> null));
    }

    private static PaimonKeyValueLookupService service(
            PaimonTableFixture fixture, LiveFileIndex index) {
        return new PaimonKeyValueLookupService(
                index,
                new CandidatePlanner(FILE_KEY_COMPARATOR, 0),
                new PaimonKeyValueDirectLookup(
                        ROW_TYPE, KEY_FIELDS, fixture.schemaId(), fixture.dataFileResolver()),
                fixture.schemaId());
    }

    private static PaimonKeyValueLookupService stringService(PaimonTableFixture fixture) {
        return new PaimonKeyValueLookupService(
                new LiveFileIndex(STRING_FILE_KEY_COMPARATOR, 4),
                new CandidatePlanner(STRING_FILE_KEY_COMPARATOR, 0),
                new PaimonKeyValueDirectLookup(
                        STRING_ROW_TYPE,
                        STRING_KEY_FIELDS,
                        fixture.schemaId(),
                        fixture.dataFileResolver()),
                fixture.schemaId());
    }

    private static void assertStringHit(
            PaimonKeyValueLookupService service, String value, int expectedAmount) throws Exception {
        LookupResult hit =
                service.lookup(
                        PARTITION,
                        BUCKET,
                        LookupRequest.projected(stringKey(value), new int[] {1}));
        assertEquals(LookupResult.Kind.HIT, hit.kind());
        assertEquals(expectedAmount, hit.row().orElseThrow().getInt(0));
    }

    private static void assertTimestampLookup(
            String tableName, RowType rowType, int precision, Timestamp timestamp) throws Exception {
        RowType fileKeyType = PrimaryKeyTableUtils.addKeyNamePrefix(rowType.project(new int[] {0}));
        Comparator<InternalRow> keyComparator = new KeyComparatorSupplier(fileKeyType).get();
        try (PaimonTableFixture fixture =
                PaimonTableFixture.createPrimaryKeyTable(tableName, rowType, List.of("event_time"))) {
            PaimonKeyValueLookupService service =
                    new PaimonKeyValueLookupService(
                            new LiveFileIndex(keyComparator, 4),
                            new CandidatePlanner(keyComparator, 0),
                            new PaimonKeyValueDirectLookup(
                                    rowType,
                                    new int[] {0},
                                    fixture.schemaId(),
                                    fixture.dataFileResolver()),
                            fixture.schemaId());
            install(service, fixture.writeRows(List.of(GenericRow.of(timestamp, 456))));

            LookupResult hit =
                    service.lookup(
                            PARTITION,
                            BUCKET,
                            LookupRequest.projected(timestampKey(timestamp, precision), new int[] {1}));
            assertEquals(LookupResult.Kind.HIT, hit.kind());
            assertEquals(456, hit.row().orElseThrow().getInt(0));
        }
    }

    private static void apply(PaimonKeyValueLookupService service, PaimonTableFixture.WriteResult delta) {
        service.applyDelta(PARTITION, BUCKET, delta.removedFiles(), delta.addedFiles());
    }

    private static void install(
            PaimonKeyValueLookupService service, PaimonTableFixture.WriteResult snapshot) {
        service.installSnapshot(PARTITION, BUCKET, snapshot.addedFiles());
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

    private static List<InternalRow> stringRows(int rowCount) {
        return java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(id -> stringRow(id, id * 10))
                .map(InternalRow.class::cast)
                .toList();
    }

    private static GenericRow stringRow(int id, int amount) {
        return GenericRow.of(BinaryString.fromString(stringValue(id)), amount);
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

    private static BinaryRow nullKey() {
        BinaryRow row = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 0);
        writer.setNullAt(1);
        writer.writeInt(2, 20_000);
        writer.complete();
        return row;
    }

    private static BinaryRow stringKey(int id) {
        return stringKey(stringValue(id));
    }

    private static BinaryRow stringKey(String value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(value));
        writer.complete();
        return row;
    }

    private static BinaryRow timestampKey(Timestamp timestamp, int precision) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeTimestamp(0, timestamp, precision);
        writer.complete();
        return row;
    }

    private static String stringValue(int id) {
        return String.format("order-%06d", id);
    }
}
