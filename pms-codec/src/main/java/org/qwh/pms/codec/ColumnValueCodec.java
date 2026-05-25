package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.qwh.pms.codec.ByteUtils.i32le;
import static org.qwh.pms.codec.ByteUtils.i64le;
import static org.qwh.pms.codec.ByteUtils.writeI16le;
import static org.qwh.pms.codec.ByteUtils.writeI32le;
import static org.qwh.pms.codec.ByteUtils.writeI64le;

/**
 * Codec for one already-located field payload.
 *
 * <p>The top-level row container decides which field payload to read. This class only translates
 * between a single payload slice and Paimon's internal Java value for the field's {@link DataType}.
 * Primitive, string, binary, decimal and timestamp values use PMS-defined stable bytes. Complex
 * values such as array, map and nested row are delegated to Paimon's internal serializers as a
 * sub-codec for that one field.
 */
public final class ColumnValueCodec {

    public byte[] encode(DataType type, InternalRow row, int fieldIndex) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeOrThrow(type, row, fieldIndex, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to encode field " + fieldIndex + " as " + type, e);
        }
    }

    public void write(DataType type, InternalRow row, int fieldIndex, ByteArrayOutputStream out) {
        try {
            writeOrThrow(type, row, fieldIndex, out);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to encode field " + fieldIndex + " as " + type, e);
        }
    }

    public Object decode(DataType type, ByteArraySlice slice) {
        try {
            return decodeOrThrow(type, slice);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode field as " + type, e);
        }
    }

    private void writeOrThrow(DataType type, InternalRow row, int fieldIndex, ByteArrayOutputStream out)
            throws IOException {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                out.write(row.getBoolean(fieldIndex) ? 1 : 0);
                return;
            case TINYINT:
                out.write(row.getByte(fieldIndex));
                return;
            case SMALLINT:
                writeI16le(out, row.getShort(fieldIndex));
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writeI32le(out, row.getInt(fieldIndex));
                return;
            case BIGINT:
                writeI64le(out, row.getLong(fieldIndex));
                return;
            case FLOAT:
                writeI32le(out, Float.floatToIntBits(row.getFloat(fieldIndex)));
                return;
            case DOUBLE:
                writeI64le(out, Double.doubleToLongBits(row.getDouble(fieldIndex)));
                return;
            case CHAR:
            case VARCHAR:
                out.writeBytes(row.getString(fieldIndex).toBytes());
                return;
            case BINARY:
            case VARBINARY:
                out.writeBytes(row.getBinary(fieldIndex));
                return;
            case DECIMAL:
                out.writeBytes(
                        row.getDecimal(
                                        fieldIndex,
                                        DataTypeChecks.getPrecision(type),
                                        DataTypeChecks.getScale(type))
                                .toUnscaledBytes());
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp timestamp = row.getTimestamp(fieldIndex, DataTypeChecks.getPrecision(type));
                writeI64le(out, timestamp.getMillisecond());
                writeI32le(out, timestamp.getNanoOfMillisecond());
                return;
            case ARRAY:
            case MAP:
            case ROW:
                out.writeBytes(serializeWithPaimon(type, valueForComplexType(type, row, fieldIndex)));
                return;
            default:
                throw new UnsupportedOperationException("Unsupported Paimon type: " + type);
        }
    }

    private Object decodeOrThrow(DataType type, ByteArraySlice slice) throws IOException {
        byte[] bytes = slice.bytes();
        int offset = slice.offset();
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                requireLength(slice, 1, type);
                return bytes[offset] != 0;
            case TINYINT:
                requireLength(slice, 1, type);
                return bytes[offset];
            case SMALLINT:
                requireLength(slice, 2, type);
                return (short) ((bytes[offset] & 0xFF) | (bytes[offset + 1] << 8));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                requireLength(slice, 4, type);
                return i32le(bytes, offset);
            case BIGINT:
                requireLength(slice, 8, type);
                return i64le(bytes, offset);
            case FLOAT:
                requireLength(slice, 4, type);
                return Float.intBitsToFloat(i32le(bytes, offset));
            case DOUBLE:
                requireLength(slice, 8, type);
                return Double.longBitsToDouble(i64le(bytes, offset));
            case CHAR:
            case VARCHAR:
                return BinaryString.fromBytes(slice.copy());
            case BINARY:
            case VARBINARY:
                return slice.copy();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromUnscaledBytes(slice.copy(), decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                requireLength(slice, 12, type);
                return Timestamp.fromEpochMillis(i64le(bytes, offset), i32le(bytes, offset + 8));
            case ARRAY:
            case MAP:
            case ROW:
                return deserializeWithPaimon(type, slice.copy());
            default:
                throw new UnsupportedOperationException("Unsupported Paimon type: " + type);
        }
    }

    private static Object valueForComplexType(DataType type, InternalRow row, int fieldIndex) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return row.getArray(fieldIndex);
            case MAP:
                return row.getMap(fieldIndex);
            case ROW:
                return row.getRow(fieldIndex, DataTypeChecks.getFieldCount(type));
            default:
                throw new IllegalArgumentException("Not a complex type: " + type);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] serializeWithPaimon(DataType type, Object value) throws IOException {
        Serializer serializer = InternalSerializers.create(type);
        return serializer.serializeToBytes(value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object deserializeWithPaimon(DataType type, byte[] bytes) throws IOException {
        Serializer serializer = InternalSerializers.create(type);
        return serializer.deserializeFromBytes(bytes);
    }

    private static void requireLength(ByteArraySlice slice, int expected, DataType type) {
        if (slice.length() != expected) {
            throw new IllegalArgumentException(
                    "Expected " + expected + " bytes for " + type + ", got " + slice.length());
        }
    }
}
