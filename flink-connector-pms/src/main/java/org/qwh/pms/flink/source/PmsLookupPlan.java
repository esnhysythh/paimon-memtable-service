package org.qwh.pms.flink.source;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.InternalRow;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.adapter.PmsFlinkRowWrapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 将 Flink LookupContext 的 key 顺序归一化为 PMS primary key 顺序.
 *
 * <p>Lookup key 的 ordinal 由 join 条件顺序决定, 不能假设它与表字段或复合主键顺序相同.
 */
final class PmsLookupPlan implements Serializable {

    private final RowType tableRowType;
    private final RowType lookupKeyRowType;
    private final RowType primaryKeyRowType;
    private final int[] primaryKeyLookupPositions;
    private final int[] extraLookupPositions;
    private final int[] extraTableIndexes;

    private transient RowDataSerializer lookupKeySerializer;
    private transient RowData.FieldGetter[] primaryKeyGetters;
    private transient RowData.FieldGetter[] extraKeyGetters;
    private transient RowData.FieldGetter[] extraResultGetters;

    private PmsLookupPlan(
            RowType tableRowType,
            RowType lookupKeyRowType,
            RowType primaryKeyRowType,
            int[] primaryKeyLookupPositions,
            int[] extraLookupPositions,
            int[] extraTableIndexes) {
        this.tableRowType = tableRowType;
        this.lookupKeyRowType = lookupKeyRowType;
        this.primaryKeyRowType = primaryKeyRowType;
        this.primaryKeyLookupPositions = primaryKeyLookupPositions;
        this.extraLookupPositions = extraLookupPositions;
        this.extraTableIndexes = extraTableIndexes;
        initialize();
    }

    static PmsLookupPlan create(int[][] keyPaths, PmsFlinkTableSchema schema) {
        if (keyPaths.length == 0) {
            throw new ValidationException("PMS Lookup Join 必须包含完整 primary key equality.");
        }
        int[] lookupTableIndexes = new int[keyPaths.length];
        Set<Integer> visitedFields = new HashSet<>();
        for (int i = 0; i < keyPaths.length; i++) {
            int[] path = keyPaths[i];
            if (path.length != 1) {
                throw new ValidationException("PMS Lookup Join 不支持 nested key path.");
            }
            int tableIndex = path[0];
            if (tableIndex < 0 || tableIndex >= schema.rowType().getFieldCount()) {
                throw new ValidationException("PMS Lookup key ordinal 越界: " + tableIndex);
            }
            if (!visitedFields.add(tableIndex)) {
                throw new ValidationException("PMS Lookup Join 不接受重复 equality 字段.");
            }
            lookupTableIndexes[i] = tableIndex;
        }

        int[] primaryKeyIndexes = schema.primaryKeyIndexes();
        int[] primaryKeyLookupPositions = new int[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyLookupPositions[i] = indexOf(lookupTableIndexes, primaryKeyIndexes[i]);
            if (primaryKeyLookupPositions[i] < 0) {
                throw new ValidationException(
                        "PMS Lookup Join 缺少 primary key 字段: "
                                + schema.primaryKeyNames().get(i));
            }
        }

        Set<Integer> primaryKeyIndexSet = new HashSet<>();
        Arrays.stream(primaryKeyIndexes).forEach(primaryKeyIndexSet::add);
        int extraCount = keyPaths.length - primaryKeyIndexes.length;
        int[] extraLookupPositions = new int[extraCount];
        int[] extraTableIndexes = new int[extraCount];
        int extraIndex = 0;
        for (int i = 0; i < lookupTableIndexes.length; i++) {
            if (!primaryKeyIndexSet.contains(lookupTableIndexes[i])) {
                extraLookupPositions[extraIndex] = i;
                extraTableIndexes[extraIndex] = lookupTableIndexes[i];
                extraIndex++;
            }
        }

        RowType lookupKeyRowType =
                new RowType(
                        false,
                        Arrays.stream(lookupTableIndexes)
                                .mapToObj(schema.rowType().getFields()::get)
                                .toList());
        return new PmsLookupPlan(
                schema.rowType(),
                lookupKeyRowType,
                schema.primaryKeyRowType(),
                primaryKeyLookupPositions,
                extraLookupPositions,
                extraTableIndexes);
    }

    RowData copyKey(RowData keyRow) {
        ensureInitialized();
        if (keyRow.getArity() != lookupKeyRowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "Lookup key arity 不匹配, expected="
                            + lookupKeyRowType.getFieldCount()
                            + ", actual="
                            + keyRow.getArity());
        }
        return lookupKeySerializer.copy(keyRow);
    }

    boolean hasNullKey(RowData keyRow) {
        for (int i = 0; i < lookupKeyRowType.getFieldCount(); i++) {
            if (keyRow.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    InternalRow toPrimaryKeyTuple(RowData keyRow) {
        ensureInitialized();
        GenericRowData tuple = new GenericRowData(primaryKeyLookupPositions.length);
        for (int i = 0; i < primaryKeyLookupPositions.length; i++) {
            tuple.setField(i, primaryKeyGetters[i].getFieldOrNull(keyRow));
        }
        return new PmsFlinkRowWrapper(tuple);
    }

    boolean matchesExtraConditions(RowData keyRow, RowData resultRow) {
        ensureInitialized();
        for (int i = 0; i < extraLookupPositions.length; i++) {
            LogicalType type = tableRowType.getTypeAt(extraTableIndexes[i]);
            Object keyValue = extraKeyGetters[i].getFieldOrNull(keyRow);
            Object resultValue = extraResultGetters[i].getFieldOrNull(resultRow);
            if (!PmsFlinkValueEqualizer.equal(type, keyValue, resultValue)) {
                return false;
            }
        }
        return true;
    }

    private void ensureInitialized() {
        if (lookupKeySerializer == null) {
            initialize();
        }
    }

    private void initialize() {
        lookupKeySerializer = new RowDataSerializer(lookupKeyRowType);
        primaryKeyGetters = new RowData.FieldGetter[primaryKeyLookupPositions.length];
        for (int i = 0; i < primaryKeyLookupPositions.length; i++) {
            primaryKeyGetters[i] =
                    RowData.createFieldGetter(
                            primaryKeyRowType.getTypeAt(i), primaryKeyLookupPositions[i]);
        }
        extraKeyGetters = new RowData.FieldGetter[extraLookupPositions.length];
        extraResultGetters = new RowData.FieldGetter[extraLookupPositions.length];
        for (int i = 0; i < extraLookupPositions.length; i++) {
            LogicalType type = tableRowType.getTypeAt(extraTableIndexes[i]);
            extraKeyGetters[i] =
                    RowData.createFieldGetter(type, extraLookupPositions[i]);
            extraResultGetters[i] =
                    RowData.createFieldGetter(type, extraTableIndexes[i]);
        }
    }

    private static int indexOf(int[] values, int expected) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == expected) {
                return i;
            }
        }
        return -1;
    }
}
