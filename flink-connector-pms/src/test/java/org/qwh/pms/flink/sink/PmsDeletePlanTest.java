package org.qwh.pms.flink.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.paimon.data.InternalRow;
import org.junit.jupiter.api.Test;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTestUtils;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsDeletePlanTest {

    @Test
    void acceptsCompletePrimaryKeyEqualitiesInAnyOperandOrder() {
        PmsFlinkTableSchema schema = compositeSchema();
        ResolvedExpression userId = equality(schema, 2, 11L, false);
        ResolvedExpression tenant = equality(schema, 0, "tenant-a", true);

        Optional<PmsDeletePlan> plan =
                PmsDeletePlan.tryCreate(List.of(tenant, userId), schema);

        assertTrue(plan.isPresent());
        InternalRow keyTuple = plan.orElseThrow().toPrimaryKeyTuple(schema);
        assertEquals(11L, keyTuple.getLong(0));
        assertEquals("tenant-a", keyTuple.getString(1).toString());
    }

    @Test
    void rejectsPartialDuplicateNonKeyAndNonEqualityFilters() {
        PmsFlinkTableSchema schema = compositeSchema();
        ResolvedExpression userId = equality(schema, 2, 11L, false);
        ResolvedExpression tenant = equality(schema, 0, "tenant-a", false);

        assertFalse(PmsDeletePlan.tryCreate(List.of(userId), schema).isPresent());
        assertFalse(
                PmsDeletePlan.tryCreate(List.of(userId, userId), schema)
                        .isPresent());
        assertFalse(
                PmsDeletePlan.tryCreate(
                                List.of(userId, equality(schema, 1, "value", false)),
                                schema)
                        .isPresent());
        assertFalse(
                PmsDeletePlan.tryCreate(
                                List.of(
                                        userId,
                                        comparison(
                                                schema,
                                                0,
                                                "tenant-a",
                                                BuiltInFunctionDefinitions.GREATER_THAN)),
                                schema)
                        .isPresent());
        assertTrue(PmsDeletePlan.tryCreate(List.of(userId, tenant), schema).isPresent());
    }

    private static ResolvedExpression equality(
            PmsFlinkTableSchema schema,
            int fieldIndex,
            Object literalValue,
            boolean literalFirst) {
        FieldReferenceExpression field = field(schema, fieldIndex);
        ValueLiteralExpression literal =
                new ValueLiteralExpression(
                        literalValue, field.getOutputDataType().notNull());
        List<ResolvedExpression> children =
                literalFirst ? List.of(literal, field) : List.of(field, literal);
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                children,
                DataTypes.BOOLEAN());
    }

    private static ResolvedExpression comparison(
            PmsFlinkTableSchema schema,
            int fieldIndex,
            Object literalValue,
            org.apache.flink.table.functions.BuiltInFunctionDefinition function) {
        FieldReferenceExpression field = field(schema, fieldIndex);
        return CallExpression.permanent(
                function,
                List.of(
                        field,
                        new ValueLiteralExpression(
                                literalValue,
                                field.getOutputDataType().notNull())),
                DataTypes.BOOLEAN());
    }

    private static FieldReferenceExpression field(
            PmsFlinkTableSchema schema, int fieldIndex) {
        RowType.RowField field = schema.rowType().getFields().get(fieldIndex);
        DataType dataType =
                TypeConversions.fromLogicalToDataType(field.getType());
        return new FieldReferenceExpression(
                field.getName(), dataType, 0, fieldIndex);
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
