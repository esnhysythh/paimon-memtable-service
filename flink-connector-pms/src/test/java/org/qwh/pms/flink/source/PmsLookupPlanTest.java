package org.qwh.pms.flink.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.InternalRow;
import org.junit.jupiter.api.Test;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsLookupPlanTest {

    @Test
    void normalizesLookupOrderAndEvaluatesExtraEqualities() {
        PmsFlinkTableSchema schema = compositeSchema();
        // Lookup key 顺序是 tenant、user_id、marker, 与 primary key 声明顺序不同.
        PmsLookupPlan plan = PmsLookupPlan.create(new int[][] {{0}, {2}, {1}}, schema);
        GenericRowData lookupKey =
                GenericRowData.of(
                        StringData.fromString("tenant-a"),
                        11L,
                        StringData.fromString("match"));

        InternalRow primaryKey = plan.toPrimaryKeyTuple(lookupKey);
        assertEquals(11L, primaryKey.getLong(0));
        assertEquals("tenant-a", primaryKey.getString(1).toString());

        RowData matching =
                GenericRowData.of(
                        StringData.fromString("tenant-a"),
                        StringData.fromString("match"),
                        11L);
        RowData different =
                GenericRowData.of(
                        StringData.fromString("tenant-a"),
                        StringData.fromString("other"),
                        11L);
        assertTrue(plan.matchesExtraConditions(lookupKey, matching));
        assertFalse(plan.matchesExtraConditions(lookupKey, different));
    }

    @Test
    void copiesReusableLookupKeysBeforeAsyncHandoff() {
        PmsLookupPlan plan =
                PmsLookupPlan.create(
                        new int[][] {{0}, {2}}, compositeSchema());
        GenericRowData reusable =
                GenericRowData.of(StringData.fromString("tenant-a"), 11L);

        RowData copied = plan.copyKey(reusable);
        reusable.setField(0, StringData.fromString("mutated"));
        reusable.setField(1, 22L);

        assertEquals("tenant-a", copied.getString(0).toString());
        assertEquals(11L, copied.getLong(1));
    }

    @Test
    void rejectsMissingPrimaryKeyAndShortCircuitsNull() {
        PmsFlinkTableSchema schema = compositeSchema();
        assertThrows(
                ValidationException.class,
                () -> PmsLookupPlan.create(new int[][] {{0}}, schema));

        PmsLookupPlan plan =
                PmsLookupPlan.create(new int[][] {{0}, {2}}, schema);
        assertTrue(
                plan.hasNullKey(
                        GenericRowData.of(
                                StringData.fromString("tenant-a"), null)));
    }

    @Test
    void extraEqualityUsesSqlTrueSemantics() {
        assertTrue(
                PmsFlinkValueEqualizer.equal(
                        DataTypes.FLOAT().getLogicalType(), -0.0f, 0.0f));

        RowType nestedType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "nullable_value",
                                                DataTypes.INT()))
                                .getLogicalType();
        assertFalse(
                PmsFlinkValueEqualizer.equal(
                        nestedType,
                        GenericRowData.of((Object) null),
                        GenericRowData.of((Object) null)));
    }

    private static PmsFlinkTableSchema compositeSchema() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "tenant",
                                                DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("marker", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "user_id",
                                                DataTypes.BIGINT().notNull()))
                                .notNull()
                                .getLogicalType();
        return PmsFlinkTestUtils.tableSchema(
                rowType, List.of("user_id", "tenant"));
    }
}
