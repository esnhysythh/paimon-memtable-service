package org.qwh.pms.flink.source;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Objects;

/** 额外 Lookup equality 条件使用的 Flink internal value 比较器. */
final class PmsFlinkValueEqualizer {

    private PmsFlinkValueEqualizer() {}

    static boolean equal(LogicalType type, Object left, Object right) {
        if (left == null || right == null) {
            // Lookup filter 只关心 equality 是否为 TRUE. SQL NULL = NULL 的结果是 UNKNOWN,
            // 对 Join 而言必须与 FALSE 一样不匹配.
            return false;
        }
        return switch (type.getTypeRoot()) {
            case FLOAT -> ((Float) left).floatValue() == ((Float) right).floatValue();
            case DOUBLE -> ((Double) left).doubleValue() == ((Double) right).doubleValue();
            case BINARY, VARBINARY -> Arrays.equals((byte[]) left, (byte[]) right);
            case ARRAY -> equalArray((ArrayType) type, (ArrayData) left, (ArrayData) right);
            case MAP -> equalMap((MapType) type, (MapData) left, (MapData) right);
            case ROW -> equalRow((RowType) type, (RowData) left, (RowData) right);
            default -> Objects.equals(left, right);
        };
    }

    private static boolean equalArray(ArrayType type, ArrayData left, ArrayData right) {
        if (left.size() != right.size()) {
            return false;
        }
        LogicalType elementType = type.getElementType();
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(elementType);
        for (int i = 0; i < left.size(); i++) {
            if (!equal(
                    elementType,
                    getter.getElementOrNull(left, i),
                    getter.getElementOrNull(right, i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalMap(MapType type, MapData left, MapData right) {
        if (left.size() != right.size()) {
            return false;
        }
        ArrayData leftKeys = left.keyArray();
        ArrayData leftValues = left.valueArray();
        ArrayData rightKeys = right.keyArray();
        ArrayData rightValues = right.valueArray();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(type.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(type.getValueType());
        boolean[] matched = new boolean[right.size()];
        for (int i = 0; i < left.size(); i++) {
            Object leftKey = keyGetter.getElementOrNull(leftKeys, i);
            Object leftValue = valueGetter.getElementOrNull(leftValues, i);
            boolean found = false;
            for (int j = 0; j < right.size(); j++) {
                if (!matched[j]
                        && equal(type.getKeyType(), leftKey, keyGetter.getElementOrNull(rightKeys, j))
                        && equal(
                                type.getValueType(),
                                leftValue,
                                valueGetter.getElementOrNull(rightValues, j))) {
                    matched[j] = true;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalRow(RowType type, RowData left, RowData right) {
        for (int i = 0; i < type.getFieldCount(); i++) {
            LogicalType fieldType = type.getTypeAt(i);
            RowData.FieldGetter getter = RowData.createFieldGetter(fieldType, i);
            if (!equal(
                    fieldType, getter.getFieldOrNull(left), getter.getFieldOrNull(right))) {
                return false;
            }
        }
        return true;
    }
}
