package org.qwh.pms.flink.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.paimon.data.InternalRow;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTypeAdapter;
import org.qwh.pms.flink.adapter.PmsFlinkRowWrapper;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/** 已验证的完整 primary key 等值 DELETE 计划. */
final class PmsDeletePlan implements Serializable {

    private final List<Object> primaryKeyValues;

    private PmsDeletePlan(List<Object> primaryKeyValues) {
        this.primaryKeyValues = List.copyOf(primaryKeyValues);
    }

    /**
     * 只接受形如 {@code pk1 = literal AND pk2 = literal} 的完整主键条件.
     *
     * <p>这里必须采用全有或全无语义. 一旦接受部分条件, Flink 就可能认为剩余行已经由 Connector
     * 正确删除, 而 PMS 又没有 Scan Source 可以补做逐行过滤.
     */
    static Optional<PmsDeletePlan> tryCreate(
            List<ResolvedExpression> filters, PmsFlinkTableSchema schema) {
        int[] primaryKeyIndexes = schema.primaryKeyIndexes();
        if (filters.size() != primaryKeyIndexes.length) {
            return Optional.empty();
        }

        Object[] values = new Object[primaryKeyIndexes.length];
        boolean[] visited = new boolean[primaryKeyIndexes.length];
        for (ResolvedExpression filter : filters) {
            FieldLiteral fieldLiteral = extractFieldLiteral(filter);
            if (fieldLiteral == null) {
                return Optional.empty();
            }

            int primaryKeyPosition =
                    primaryKeyPosition(primaryKeyIndexes, fieldLiteral.field().getFieldIndex());
            if (fieldLiteral.field().getInputIndex() != 0
                    || primaryKeyPosition < 0
                    || visited[primaryKeyPosition]) {
                return Optional.empty();
            }

            LogicalType expectedType = schema.primaryKeyTypes().get(primaryKeyPosition);
            ValueLiteralExpression literal = fieldLiteral.literal();
            if (literal.isNull()
                    || expectedType.getTypeRoot()
                            != literal.getOutputDataType().getLogicalType().getTypeRoot()) {
                return Optional.empty();
            }

            Object value = extractLiteralValue(literal, expectedType);
            if (value == null) {
                return Optional.empty();
            }
            values[primaryKeyPosition] = value;
            visited[primaryKeyPosition] = true;
        }

        for (boolean keyVisited : visited) {
            if (!keyVisited) {
                return Optional.empty();
            }
        }
        return Optional.of(new PmsDeletePlan(List.of(values)));
    }

    InternalRow toPrimaryKeyTuple(PmsFlinkTableSchema schema) {
        List<LogicalType> primaryKeyTypes = schema.primaryKeyTypes();
        GenericRowData keyTuple = new GenericRowData(primaryKeyValues.size());
        for (int i = 0; i < primaryKeyValues.size(); i++) {
            keyTuple.setField(
                    i,
                    PmsFlinkTypeAdapter.toFlinkInternalLiteral(
                            primaryKeyValues.get(i), primaryKeyTypes.get(i)));
        }
        return new PmsFlinkRowWrapper(keyTuple);
    }

    private static FieldLiteral extractFieldLiteral(ResolvedExpression expression) {
        if (!(expression instanceof CallExpression call)
                || call.getFunctionDefinition() != BuiltInFunctionDefinitions.EQUALS) {
            return null;
        }
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (children.size() != 2) {
            return null;
        }
        if (children.get(0) instanceof FieldReferenceExpression field
                && children.get(1) instanceof ValueLiteralExpression literal) {
            return new FieldLiteral(field, literal);
        }
        if (children.get(0) instanceof ValueLiteralExpression literal
                && children.get(1) instanceof FieldReferenceExpression field) {
            return new FieldLiteral(field, literal);
        }
        return null;
    }

    private static Object extractLiteralValue(
            ValueLiteralExpression literal, LogicalType expectedType) {
        return switch (expectedType.getTypeRoot()) {
            case INTEGER -> literal.getValueAs(Integer.class).orElse(null);
            case BIGINT -> literal.getValueAs(Long.class).orElse(null);
            case DATE -> literal.getValueAs(LocalDate.class).orElse(null);
            case VARCHAR -> literal.getValueAs(String.class).orElse(null);
            case TIMESTAMP_WITHOUT_TIME_ZONE ->
                    literal.getValueAs(LocalDateTime.class).orElse(null);
            default -> null;
        };
    }

    private static int primaryKeyPosition(int[] primaryKeyIndexes, int fieldIndex) {
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            if (primaryKeyIndexes[i] == fieldIndex) {
                return i;
            }
        }
        return -1;
    }

    private record FieldLiteral(
            FieldReferenceExpression field, ValueLiteralExpression literal) {}
}
