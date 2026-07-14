package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;

import java.util.List;
import java.util.Map;

/**
 * Builds Paimon delete rows from PMS key-only tombstones.
 *
 * <p>PMS deliberately stores no row value in a tombstone and does not read the old Paimon row
 * before sinking a delete. Ideally the resulting {@link RowKind#DELETE} row would therefore contain
 * only primary-key fields. Paimon 1.4.1 currently checks every top-level {@code NOT NULL} field
 * before interpreting the row kind (apache/paimon#4702), so a true key-only row is rejected.
 *
 * <p>This factory is the single compatibility boundary for that mismatch. It preserves decoded
 * primary-key values, leaves nullable non-key fields null, and supplies deterministic synthetic
 * values for non-key {@code NOT NULL} fields. Those values are transport placeholders only: they
 * are not schema defaults, old values, or meaningful delete changelog data. Empty collections and
 * recursively populated rows are used for complex types so nested nullability checks also pass.
 * Remove the synthetic-value path when Paimon accepts key-only delete rows.
 */
final class DeleteRowFactory {
    private final RowType rowType;
    private final PmsPrimaryKeyCodec primaryKeyCodec;
    private final int[] primaryKeyOrdinals;
    private final InternalRow.FieldGetter[] primaryKeyGetters;
    private final Object[] syntheticValues;

    DeleteRowFactory(RowType rowType, List<String> primaryKeyNames) {
        if (rowType == null) {
            throw new NullPointerException("rowType must not be null");
        }
        if (primaryKeyNames == null || primaryKeyNames.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyNames must not be empty");
        }

        this.rowType = rowType;
        this.primaryKeyOrdinals = new int[primaryKeyNames.size()];
        this.primaryKeyGetters = new InternalRow.FieldGetter[primaryKeyNames.size()];

        boolean[] primaryKeyOrdinalSet = new boolean[rowType.getFieldCount()];
        for (int i = 0; i < primaryKeyNames.size(); i++) {
            String fieldName = primaryKeyNames.get(i);
            int ordinal = rowType.getFieldIndex(fieldName);
            if (ordinal < 0) {
                throw new IllegalArgumentException("Unknown primary key field: " + fieldName);
            }
            DataField field = rowType.getFields().get(ordinal);
            primaryKeyOrdinals[i] = ordinal;
            primaryKeyGetters[i] = InternalRow.createFieldGetter(field.type(), i);
            primaryKeyOrdinalSet[ordinal] = true;
        }

        this.primaryKeyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeyNames);
        this.syntheticValues = syntheticValues(rowType, primaryKeyOrdinalSet);
    }

    InternalRow createDeleteRow(byte[] keyBytes) {
        InternalRow keyTuple = primaryKeyCodec.decodeKey(keyBytes);
        GenericRow row = new GenericRow(RowKind.DELETE, rowType.getFieldCount());
        for (int i = 0; i < syntheticValues.length; i++) {
            Object value = syntheticValues[i];
            if (value != null) {
                row.setField(i, value);
            }
        }
        for (int i = 0; i < primaryKeyOrdinals.length; i++) {
            row.setField(primaryKeyOrdinals[i], primaryKeyGetters[i].getFieldOrNull(keyTuple));
        }
        return row;
    }

    private static Object[] syntheticValues(RowType rowType, boolean[] primaryKeyOrdinalSet) {
        Object[] values = new Object[rowType.getFieldCount()];
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (!primaryKeyOrdinalSet[i] && !field.type().isNullable()) {
                values[i] = syntheticValue(field);
            }
        }
        return values;
    }

    private static Object syntheticValue(DataField field) {
        return syntheticValue(field.type(), field.name());
    }

    private static Object syntheticValue(DataType type, String fieldPath) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return false;
            case TINYINT:
                return (byte) 0;
            case SMALLINT:
                return (short) 0;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 0;
            case BIGINT:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0d;
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString("");
            case BINARY:
            case VARBINARY:
                return new byte[0];
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.zero(decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.fromEpochMillis(0);
            case ARRAY:
                return new GenericArray(new Object[0]);
            case MULTISET:
            case MAP:
                return new GenericMap(Map.of());
            case ROW:
                return syntheticRow((RowType) type, fieldPath);
            default:
                throw new UnsupportedOperationException(
                    "Cannot synthesize DELETE value for NOT NULL field "
                        + fieldPath
                        + " of type "
                        + type
                );
        }
    }

    private static InternalRow syntheticRow(RowType rowType, String fieldPath) {
        GenericRow row = new GenericRow(rowType.getFieldCount());
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (!field.type().isNullable()) {
                row.setField(i, syntheticValue(field.type(), fieldPath + "." + field.name()));
            }
        }
        return row;
    }
}
