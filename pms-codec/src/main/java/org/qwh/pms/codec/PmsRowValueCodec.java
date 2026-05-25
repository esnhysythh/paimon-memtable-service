package org.qwh.pms.codec;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.qwh.pms.codec.ByteUtils.FLAG_LARGE_ROW;
import static org.qwh.pms.codec.ByteUtils.HEADER_SIZE;
import static org.qwh.pms.codec.ByteUtils.VERSION;
import static org.qwh.pms.codec.ByteUtils.writeI32le;
import static org.qwh.pms.codec.ByteUtils.writeU16le;
import static org.qwh.pms.codec.ByteUtils.writeU8;

/**
 * Top-level PMS row value codec.
 *
 * <p>This class owns the TiDB-like row container format: header, field-id indexes, end offsets and
 * payload bytes. It deliberately does not use Paimon's {@code InternalRowSerializer} for the top
 * level row, because PMS needs to parse metadata first and decode only projected fields later.
 *
 * <p>Typical read flow is {@link #parse(byte[])} to get a lightweight {@link RowValueView}, then
 * {@link #decodeProjected(RowType, byte[], int[])} when only part of the row is needed. {@link
 * #decode(RowType, byte[])} is the full-row convenience path.
 */
public final class PmsRowValueCodec {

    private final ColumnValueCodec columnValueCodec;

    public PmsRowValueCodec() {
        this(new ColumnValueCodec());
    }

    public PmsRowValueCodec(ColumnValueCodec columnValueCodec) {
        this.columnValueCodec = columnValueCodec;
    }

    public byte[] encode(RowType writerType, InternalRow row, int writerSchemaId) {
        if (row.getFieldCount() != writerType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "Row arity "
                            + row.getFieldCount()
                            + " does not match type field count "
                            + writerType.getFieldCount());
        }

        RowKind rowKind = normalize(row.getRowKind());
        if (rowKind == RowKind.DELETE) {
            return encodeHeader(0, rowKind, writerSchemaId, 0, 0);
        }

        List<FieldRef> notNullFields = new ArrayList<>();
        List<Integer> nullFieldIds = new ArrayList<>();
        List<DataField> fields = writerType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            int fieldId = field.id();
            if (fieldId < 0) {
                throw new IllegalArgumentException("Field id must be non-negative: " + fieldId);
            }
            if (row.isNullAt(i)) {
                nullFieldIds.add(fieldId);
            } else {
                notNullFields.add(new FieldRef(fieldId, field, i));
            }
        }

        notNullFields.sort(Comparator.comparingInt(FieldRef::fieldId));
        nullFieldIds.sort(Integer::compareTo);
        validateUniqueIds(notNullFields, nullFieldIds);

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        int[] endOffsets = new int[notNullFields.size()];
        for (int i = 0; i < notNullFields.size(); i++) {
            FieldRef field = notNullFields.get(i);
            columnValueCodec.write(field.dataField().type(), row, field.fieldIndex(), payload);
            endOffsets[i] = payload.size();
        }

        int maxFieldId = 0;
        for (FieldRef field : notNullFields) {
            maxFieldId = Math.max(maxFieldId, field.fieldId());
        }
        for (int fieldId : nullFieldIds) {
            maxFieldId = Math.max(maxFieldId, fieldId);
        }

        int payloadLength = payload.size();
        boolean largeRow = maxFieldId > 255 || payloadLength > 65535;
        int flags = largeRow ? FLAG_LARGE_ROW : 0;
        int idWidth = largeRow ? 4 : 1;
        int offsetWidth = largeRow ? 4 : 2;

        ByteArrayOutputStream out =
                new ByteArrayOutputStream(
                        HEADER_SIZE
                                + (notNullFields.size() + nullFieldIds.size()) * idWidth
                                + notNullFields.size() * offsetWidth
                                + payloadLength);
        writeHeader(out, flags, rowKind, writerSchemaId, notNullFields.size(), nullFieldIds.size());

        for (FieldRef field : notNullFields) {
            writeUnsigned(out, field.fieldId(), idWidth);
        }
        for (int fieldId : nullFieldIds) {
            writeUnsigned(out, fieldId, idWidth);
        }

        for (int endOffset : endOffsets) {
            writeUnsigned(out, endOffset, offsetWidth);
        }
        writePayload(out, payload);
        return out.toByteArray();
    }

    public RowValueView parse(byte[] rowValue) {
        return RowValueView.parse(rowValue);
    }

    public RowValueView parse(byte[] rowValue, ValidationMode validationMode) {
        return RowValueView.parse(rowValue, validationMode);
    }

    public InternalRow decode(RowType readType, byte[] rowValue) {
        return decode(readType, rowValue, ValidationMode.TRUSTED);
    }

    public InternalRow decode(RowType readType, byte[] rowValue, ValidationMode validationMode) {
        RowValueView view = parse(rowValue, validationMode);
        if (view.isTombstone()) {
            GenericRow row = new GenericRow(RowKind.DELETE, readType.getFieldCount());
            return row;
        }
        GenericRow row = new GenericRow(view.rowKind(), readType.getFieldCount());
        for (int i = 0; i < readType.getFields().size(); i++) {
            decodeFieldInto(row, i, readType.getFields().get(i), view);
        }
        return row;
    }

    public InternalRow decodeProjected(RowType readType, byte[] rowValue, int[] projectedFieldIds) {
        return decodeProjected(readType, rowValue, projectedFieldIds, ValidationMode.TRUSTED);
    }

    public InternalRow decodeProjected(RowType readType, byte[] rowValue, String[] projectedFieldNames) {
        int[] projectedFieldIds = new int[projectedFieldNames.length];
        for (int i = 0; i < projectedFieldNames.length; i++) {
            projectedFieldIds[i] = readType.getField(projectedFieldNames[i]).id();
        }
        return decodeProjected(readType, rowValue, projectedFieldIds);
    }

    public InternalRow decodeProjected(
            RowType readType, byte[] rowValue, int[] projectedFieldIds, ValidationMode validationMode) {
        RowValueView view = parse(rowValue, validationMode);
        GenericRow row = new GenericRow(view.rowKind(), projectedFieldIds.length);
        if (view.isTombstone()) {
            row.setRowKind(RowKind.DELETE);
            return row;
        }
        for (int outputIndex = 0; outputIndex < projectedFieldIds.length; outputIndex++) {
            DataField field = readType.getField(projectedFieldIds[outputIndex]);
            decodeFieldInto(row, outputIndex, field, view);
        }
        return row;
    }

    private void decodeFieldInto(GenericRow row, int outputIndex, DataField field, RowValueView view) {
        FieldLookup lookup = view.lookup(field.id());
        switch (lookup.kind()) {
            case NOT_NULL:
                row.setField(
                        outputIndex,
                        columnValueCodec.decode(field.type(), view.payloadSlice(lookup.notNullIndex())));
                return;
            case NULL:
                row.setField(outputIndex, null);
                return;
            case MISSING:
                row.setField(outputIndex, missingValue(field));
                return;
            default:
                throw new IllegalStateException("Unexpected lookup kind: " + lookup.kind());
        }
    }

    private Object missingValue(DataField field) {
        if (field.defaultValue() != null) {
            throw new UnsupportedOperationException(
                    "Default value decoding is not implemented yet for missing field " + field.name());
        }
        if (field.type().isNullable()) {
            return null;
        }
        throw new IllegalArgumentException("Missing non-null field without default value: " + field.name());
    }

    private static RowKind normalize(RowKind rowKind) {
        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            return RowKind.INSERT;
        }
        if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
            return RowKind.DELETE;
        }
        throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
    }

    private static byte[] encodeHeader(
            int flags, RowKind rowKind, int writerSchemaId, int notNullCount, int nullCount) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_SIZE);
        writeHeader(out, flags, rowKind, writerSchemaId, notNullCount, nullCount);
        return out.toByteArray();
    }

    private static void writeHeader(
            ByteArrayOutputStream out,
            int flags,
            RowKind rowKind,
            int writerSchemaId,
            int notNullCount,
            int nullCount) {
        if (notNullCount > 65535 || nullCount > 65535) {
            throw new IllegalArgumentException("Field count exceeds uint16 limit");
        }
        writeU8(out, VERSION);
        writeU8(out, flags);
        writeU8(out, rowKind.toByteValue());
        writeU8(out, 0);
        writeI32le(out, writerSchemaId);
        writeU16le(out, notNullCount);
        writeU16le(out, nullCount);
    }

    private static void writeUnsigned(ByteArrayOutputStream out, int value, int width) {
        if (width == 1) {
            if (value > 255) {
                throw new IllegalArgumentException("Value exceeds uint8: " + value);
            }
            writeU8(out, value);
        } else if (width == 2) {
            if (value > 65535) {
                throw new IllegalArgumentException("Value exceeds uint16: " + value);
            }
            writeU16le(out, value);
        } else if (width == 4) {
            writeI32le(out, value);
        } else {
            throw new IllegalArgumentException("Unsupported unsigned width: " + width);
        }
    }

    private static void writePayload(ByteArrayOutputStream out, ByteArrayOutputStream payload) {
        try {
            payload.writeTo(out);
        } catch (IOException e) {
            throw new IllegalStateException("ByteArrayOutputStream unexpectedly failed", e);
        }
    }

    private static void validateUniqueIds(List<FieldRef> notNullFields, List<Integer> nullFieldIds) {
        Integer previous = null;
        for (FieldRef field : notNullFields) {
            if (previous != null && previous == field.fieldId()) {
                throw new IllegalArgumentException("Duplicate field id: " + field.fieldId());
            }
            previous = field.fieldId();
        }
        previous = null;
        for (int fieldId : nullFieldIds) {
            if (previous != null && previous == fieldId) {
                throw new IllegalArgumentException("Duplicate field id: " + fieldId);
            }
            previous = fieldId;
        }
        int i = 0;
        int j = 0;
        while (i < notNullFields.size() && j < nullFieldIds.size()) {
            int left = notNullFields.get(i).fieldId();
            int right = nullFieldIds.get(j);
            if (left == right) {
                throw new IllegalArgumentException("Field id appears as both null and not-null: " + left);
            }
            if (left < right) {
                i++;
            } else {
                j++;
            }
        }
    }

    private record FieldRef(int fieldId, DataField dataField, int fieldIndex) {}
}
