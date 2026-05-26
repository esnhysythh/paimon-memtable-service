package org.qwh.pms.server;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class JsonRowMapper {
    private final RowType rowType;

    JsonRowMapper(RowType rowType) {
        this.rowType = rowType;
    }

    GenericRow fullRow(Map<String, Object> values, RowKind rowKind) {
        GenericRow row = new GenericRow(rowKind, rowType.getFieldCount());
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            Object value = values.get(field.name());
            if (value == null) {
                row.setField(i, null);
            } else {
                row.setField(i, toPaimonValue(field.type(), value));
            }
        }
        return row;
    }

    GenericRow keyTuple(Map<String, Object> values, List<String> primaryKeys) {
        GenericRow row = new GenericRow(RowKind.INSERT, primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++) {
            String fieldName = primaryKeys.get(i);
            DataField field = rowType.getField(fieldName);
            if (!values.containsKey(fieldName) || values.get(fieldName) == null) {
                throw new IllegalArgumentException("Missing primary key field: " + fieldName);
            }
            row.setField(i, toPaimonValue(field.type(), values.get(fieldName)));
        }
        return row;
    }

    Map<String, Object> toJsonObject(InternalRow row) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            result.put(field.name(), row.isNullAt(i) ? null : toJsonValue(row, i, field.type()));
        }
        return result;
    }

    private static Object toPaimonValue(DataType type, Object value) {
        return switch (type.getTypeRoot()) {
            case BOOLEAN -> requireBoolean(value);
            case TINYINT -> (byte) requireLong(value);
            case SMALLINT -> (short) requireLong(value);
            case INTEGER, DATE, TIME_WITHOUT_TIME_ZONE -> (int) requireLong(value);
            case BIGINT -> requireLong(value);
            case FLOAT -> (float) requireDouble(value);
            case DOUBLE -> requireDouble(value);
            case CHAR, VARCHAR -> BinaryString.fromString(String.valueOf(value));
            case BINARY, VARBINARY -> decodeBinary(value);
            case DECIMAL -> {
                BigDecimal decimal = value instanceof Number
                    ? BigDecimal.valueOf(requireDouble(value))
                    : new BigDecimal(String.valueOf(value));
                yield Decimal.fromBigDecimal(
                    decimal,
                    DataTypeChecks.getPrecision(type),
                    DataTypeChecks.getScale(type)
                );
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                Timestamp.fromEpochMillis(requireLong(value));
            default -> throw new UnsupportedOperationException("Unsupported JSON field type: " + type);
        };
    }

    private static Object toJsonValue(InternalRow row, int index, DataType type) {
        return switch (type.getTypeRoot()) {
            case BOOLEAN -> row.getBoolean(index);
            case TINYINT -> row.getByte(index);
            case SMALLINT -> row.getShort(index);
            case INTEGER, DATE, TIME_WITHOUT_TIME_ZONE -> row.getInt(index);
            case BIGINT -> row.getLong(index);
            case FLOAT -> row.getFloat(index);
            case DOUBLE -> row.getDouble(index);
            case CHAR, VARCHAR -> row.getString(index).toString();
            case BINARY, VARBINARY -> Base64.getEncoder().encodeToString(row.getBinary(index));
            case DECIMAL -> row.getDecimal(
                index,
                DataTypeChecks.getPrecision(type),
                DataTypeChecks.getScale(type)
            ).toBigDecimal().toPlainString();
            case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                row.getTimestamp(index, DataTypeChecks.getPrecision(type)).getMillisecond();
            default -> throw new UnsupportedOperationException("Unsupported JSON field type: " + type);
        };
    }

    private static boolean requireBoolean(Object value) {
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalArgumentException("Expected boolean value, got " + value);
    }

    private static long requireLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static double requireDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    private static byte[] decodeBinary(Object value) {
        if (value instanceof String s) {
            return Base64.getDecoder().decode(s);
        }
        throw new IllegalArgumentException("Binary fields must be base64 strings");
    }
}
