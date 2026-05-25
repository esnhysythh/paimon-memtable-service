package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * PMS primary key codec.
 *
 * <p>The encoded bytes are ordered for unsigned lexicographical comparison, which is the ordering
 * used by pms-core MemTable and SST keys. The tuple layout follows the TiDB CommonHandle idea:
 * each primary key field is encoded as a self-delimiting, mem-comparable datum and fields are
 * concatenated in primary-key order.
 */
public final class PmsPrimaryKeyCodec {

    private static final int FLAG_BYTES = 0x01;
    private static final int FLAG_INT8 = 0x03;
    private static final int FLAG_INT16 = 0x04;
    private static final int FLAG_INT32 = 0x05;
    private static final int FLAG_INT64 = 0x06;
    private static final int FLAG_TIMESTAMP_MILLIS = 0x07;
    private static final int FLAG_TIMESTAMP_NANOS = 0x08;
    private static final int FLAG_DATE_DAYS = 0x09;
    private static final int FLAG_TIME_MILLIS = 0x0A;

    private static final int ENCODED_GROUP_SIZE = 8;
    private static final int ENCODED_MARKER = 0xFF;
    private static final int ENCODED_PAD = 0x00;
    private static final int MILLIS_PER_DAY = 86_400_000;

    private final RowType rowType;
    private final int[] primaryKeyFieldIds;
    private final int[] primaryKeyOrdinals;
    private final DataField[] primaryKeyFields;

    public PmsPrimaryKeyCodec(RowType rowType, int[] primaryKeyFieldIds) {
        if (rowType == null) {
            throw new NullPointerException("rowType must not be null");
        }
        if (primaryKeyFieldIds == null) {
            throw new NullPointerException("primaryKeyFieldIds must not be null");
        }
        if (primaryKeyFieldIds.length == 0) {
            throw new IllegalArgumentException("Primary key must contain at least one field");
        }
        this.rowType = rowType;
        this.primaryKeyFieldIds = Arrays.copyOf(primaryKeyFieldIds, primaryKeyFieldIds.length);
        this.primaryKeyOrdinals = new int[primaryKeyFieldIds.length];
        this.primaryKeyFields = new DataField[primaryKeyFieldIds.length];

        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < primaryKeyFieldIds.length; i++) {
            int fieldId = primaryKeyFieldIds[i];
            if (!seen.add(fieldId)) {
                throw new IllegalArgumentException("Duplicate primary key field id: " + fieldId);
            }
            DataField field = rowType.getField(fieldId);
            requireSupported(field);
            primaryKeyFields[i] = field;
            primaryKeyOrdinals[i] = rowType.getFieldIndexByFieldId(fieldId);
        }
    }

    public static PmsPrimaryKeyCodec forFieldNames(
            RowType rowType, List<String> primaryKeyFieldNames) {
        if (primaryKeyFieldNames == null) {
            throw new NullPointerException("primaryKeyFieldNames must not be null");
        }
        int[] fieldIds = new int[primaryKeyFieldNames.size()];
        for (int i = 0; i < primaryKeyFieldNames.size(); i++) {
            fieldIds[i] = rowType.getField(primaryKeyFieldNames.get(i)).id();
        }
        return new PmsPrimaryKeyCodec(rowType, fieldIds);
    }

    public byte[] encodeKey(InternalRow fullRow) {
        requireFullRowArity(fullRow);
        return encodeFullRow(fullRow, primaryKeyFields.length);
    }

    public byte[] encodePrefix(InternalRow fullRow, int primaryKeyFieldCount) {
        requireFullRowArity(fullRow);
        requirePrefixCount(primaryKeyFieldCount);
        return encodeFullRow(fullRow, primaryKeyFieldCount);
    }

    public byte[] encodeKeyTuple(InternalRow keyTuple) {
        if (keyTuple.getFieldCount() != primaryKeyFields.length) {
            throw new IllegalArgumentException(
                    "Key tuple arity "
                            + keyTuple.getFieldCount()
                            + " does not match primary key field count "
                            + primaryKeyFields.length);
        }
        return encodeTuple(keyTuple, primaryKeyFields.length);
    }

    public byte[] encodePrefixTuple(InternalRow keyPrefixTuple) {
        int fieldCount = keyPrefixTuple.getFieldCount();
        requirePrefixCount(fieldCount);
        return encodeTuple(keyPrefixTuple, fieldCount);
    }

    public int[] primaryKeyFieldIds() {
        return Arrays.copyOf(primaryKeyFieldIds, primaryKeyFieldIds.length);
    }

    public InternalRow decodeKey(byte[] key) {
        return decodePrefix(key, primaryKeyFields.length);
    }

    public InternalRow decodePrefix(byte[] prefix, int primaryKeyFieldCount) {
        if (prefix == null) {
            throw new NullPointerException("prefix must not be null");
        }
        requirePrefixCount(primaryKeyFieldCount);

        Cursor cursor = new Cursor(prefix);
        GenericRow row = new GenericRow(RowKind.INSERT, primaryKeyFieldCount);
        for (int i = 0; i < primaryKeyFieldCount; i++) {
            row.setField(i, readField(cursor, primaryKeyFields[i].type()));
        }
        if (cursor.hasRemaining()) {
            throw new IllegalArgumentException("Unexpected trailing bytes after primary key fields");
        }
        return row;
    }

    public static Optional<byte[]> prefixNext(byte[] encodedPrefix) {
        if (encodedPrefix == null) {
            throw new NullPointerException("encodedPrefix must not be null");
        }
        byte[] next = Arrays.copyOf(encodedPrefix, encodedPrefix.length);
        for (int i = next.length - 1; i >= 0; i--) {
            int value = next[i] & 0xFF;
            if (value != 0xFF) {
                next[i] = (byte) (value + 1);
                return Optional.of(Arrays.copyOf(next, i + 1));
            }
        }
        return Optional.empty();
    }

    private byte[] encodeFullRow(InternalRow row, int primaryKeyFieldCount) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < primaryKeyFieldCount; i++) {
            int fieldOrdinal = primaryKeyOrdinals[i];
            if (row.isNullAt(fieldOrdinal)) {
                throw new IllegalArgumentException(
                        "Primary key field must not be null: " + primaryKeyFields[i].name());
            }
            writeField(out, primaryKeyFields[i].type(), row, fieldOrdinal);
        }
        return out.toByteArray();
    }

    private byte[] encodeTuple(InternalRow row, int fieldCount) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                throw new IllegalArgumentException(
                        "Primary key field must not be null: " + primaryKeyFields[i].name());
            }
            writeField(out, primaryKeyFields[i].type(), row, i);
        }
        return out.toByteArray();
    }

    private Object readField(Cursor cursor, DataType type) {
        int flag = cursor.readU8("type flag");
        switch (type.getTypeRoot()) {
            case TINYINT:
                requireFlag(flag, FLAG_INT8, type);
                return readOrderedInt8(cursor);
            case SMALLINT:
                requireFlag(flag, FLAG_INT16, type);
                return readOrderedInt16(cursor);
            case INTEGER:
                requireFlag(flag, FLAG_INT32, type);
                return readOrderedInt32(cursor);
            case BIGINT:
                requireFlag(flag, FLAG_INT64, type);
                return readOrderedInt64(cursor);
            case DATE:
                requireFlag(flag, FLAG_DATE_DAYS, type);
                return readOrderedInt32(cursor);
            case TIME_WITHOUT_TIME_ZONE:
                requireFlag(flag, FLAG_TIME_MILLIS, type);
                return readTimeMillis(cursor);
            case CHAR:
            case VARCHAR:
                requireFlag(flag, FLAG_BYTES, type);
                return BinaryString.fromBytes(readComparableBytes(cursor));
            case BINARY:
            case VARBINARY:
                requireFlag(flag, FLAG_BYTES, type);
                return readComparableBytes(cursor);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return readTimestamp(cursor, flag, type);
            default:
                throw new UnsupportedOperationException("Unsupported primary key type: " + type);
        }
    }

    private void writeField(ByteArrayOutputStream out, DataType type, InternalRow row, int fieldIndex) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                writeU8(out, FLAG_INT8);
                writeOrderedInt8(out, row.getByte(fieldIndex));
                return;
            case SMALLINT:
                writeU8(out, FLAG_INT16);
                writeOrderedInt16(out, row.getShort(fieldIndex));
                return;
            case INTEGER:
                writeU8(out, FLAG_INT32);
                writeOrderedInt32(out, row.getInt(fieldIndex));
                return;
            case BIGINT:
                writeU8(out, FLAG_INT64);
                writeOrderedInt64(out, row.getLong(fieldIndex));
                return;
            case DATE:
                writeU8(out, FLAG_DATE_DAYS);
                writeOrderedInt32(out, row.getInt(fieldIndex));
                return;
            case TIME_WITHOUT_TIME_ZONE:
                writeU8(out, FLAG_TIME_MILLIS);
                writeTimeMillis(out, row.getInt(fieldIndex));
                return;
            case CHAR:
            case VARCHAR:
                writeU8(out, FLAG_BYTES);
                writeComparableBytes(out, row.getString(fieldIndex).toBytes());
                return;
            case BINARY:
            case VARBINARY:
                writeU8(out, FLAG_BYTES);
                writeComparableBytes(out, row.getBinary(fieldIndex));
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                writeTimestamp(out, type, row.getTimestamp(fieldIndex, DataTypeChecks.getPrecision(type)));
                return;
            default:
                throw new UnsupportedOperationException("Unsupported primary key type: " + type);
        }
    }

    private static void writeTimestamp(ByteArrayOutputStream out, DataType type, Timestamp timestamp) {
        int precision = DataTypeChecks.getPrecision(type);
        if (precision <= 3) {
            if (timestamp.getNanoOfMillisecond() != 0) {
                throw new IllegalArgumentException(
                        "Timestamp precision "
                                + precision
                                + " cannot encode non-zero nano-of-millisecond: "
                                + timestamp.getNanoOfMillisecond());
            }
            writeU8(out, FLAG_TIMESTAMP_MILLIS);
            writeOrderedInt64(out, timestamp.getMillisecond());
            return;
        }
        writeU8(out, FLAG_TIMESTAMP_NANOS);
        writeOrderedInt64(out, timestamp.getMillisecond());
        writeUnsignedInt32(out, timestamp.getNanoOfMillisecond());
    }

    private static void writeComparableBytes(ByteArrayOutputStream out, byte[] bytes) {
        int offset = 0;
        while (true) {
            int remaining = bytes.length - offset;
            int copyLength = Math.min(remaining, ENCODED_GROUP_SIZE);
            out.write(bytes, offset, copyLength);
            for (int i = copyLength; i < ENCODED_GROUP_SIZE; i++) {
                out.write(ENCODED_PAD);
            }
            int padCount = ENCODED_GROUP_SIZE - copyLength;
            out.write(ENCODED_MARKER - padCount);
            offset += copyLength;
            if (copyLength < ENCODED_GROUP_SIZE) {
                return;
            }
        }
    }

    private static void writeTimeMillis(ByteArrayOutputStream out, int millisOfDay) {
        if (millisOfDay < 0 || millisOfDay >= MILLIS_PER_DAY) {
            throw new IllegalArgumentException("TIME millis-of-day out of range: " + millisOfDay);
        }
        writeUnsignedInt32(out, millisOfDay);
    }

    private static Timestamp readTimestamp(Cursor cursor, int flag, DataType type) {
        int precision = DataTypeChecks.getPrecision(type);
        if (precision <= 3) {
            requireFlag(flag, FLAG_TIMESTAMP_MILLIS, type);
            return Timestamp.fromEpochMillis(readOrderedInt64(cursor));
        }
        requireFlag(flag, FLAG_TIMESTAMP_NANOS, type);
        long millisecond = readOrderedInt64(cursor);
        int nanoOfMillisecond = readUnsignedInt32(cursor);
        if (nanoOfMillisecond < 0 || nanoOfMillisecond > 999_999) {
            throw new IllegalArgumentException(
                    "Timestamp nano-of-millisecond out of range: " + nanoOfMillisecond);
        }
        return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    private static byte[] readComparableBytes(Cursor cursor) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            cursor.requireAvailable(ENCODED_GROUP_SIZE + 1, "mem-comparable bytes group");
            int groupOffset = cursor.position();
            cursor.skip(ENCODED_GROUP_SIZE);
            int marker = cursor.readU8("mem-comparable bytes marker");
            int padCount = ENCODED_MARKER - marker;
            if (padCount < 0 || padCount > ENCODED_GROUP_SIZE) {
                throw new IllegalArgumentException("Invalid mem-comparable bytes marker: " + marker);
            }
            int rawLength = ENCODED_GROUP_SIZE - padCount;
            out.write(cursor.bytes(), groupOffset, rawLength);
            if (padCount > 0) {
                for (int i = rawLength; i < ENCODED_GROUP_SIZE; i++) {
                    if ((cursor.bytes()[groupOffset + i] & 0xFF) != ENCODED_PAD) {
                        throw new IllegalArgumentException("Invalid non-zero padding in mem-comparable bytes");
                    }
                }
                return out.toByteArray();
            }
        }
    }

    private static int readTimeMillis(Cursor cursor) {
        int millisOfDay = readUnsignedInt32(cursor);
        if (millisOfDay < 0 || millisOfDay >= MILLIS_PER_DAY) {
            throw new IllegalArgumentException("TIME millis-of-day out of range: " + millisOfDay);
        }
        return millisOfDay;
    }

    private static byte readOrderedInt8(Cursor cursor) {
        return (byte) (cursor.readU8("int8 payload") ^ 0x80);
    }

    private static short readOrderedInt16(Cursor cursor) {
        return (short) (readUnsignedInt16(cursor) ^ 0x8000);
    }

    private static int readOrderedInt32(Cursor cursor) {
        return readUnsignedInt32(cursor) ^ 0x80000000;
    }

    private static long readOrderedInt64(Cursor cursor) {
        return readUnsignedInt64(cursor) ^ 0x8000000000000000L;
    }

    private static void writeOrderedInt8(ByteArrayOutputStream out, byte value) {
        writeU8(out, (value ^ 0x80) & 0xFF);
    }

    private static void writeOrderedInt16(ByteArrayOutputStream out, short value) {
        writeUnsignedInt16(out, (value ^ 0x8000) & 0xFFFF);
    }

    private static void writeOrderedInt32(ByteArrayOutputStream out, int value) {
        writeUnsignedInt32(out, value ^ 0x80000000);
    }

    private static void writeOrderedInt64(ByteArrayOutputStream out, long value) {
        writeUnsignedInt64(out, value ^ 0x8000000000000000L);
    }

    private static int readUnsignedInt16(Cursor cursor) {
        cursor.requireAvailable(2, "uint16 payload");
        int offset = cursor.position();
        cursor.skip(2);
        return ((cursor.bytes()[offset] & 0xFF) << 8) | (cursor.bytes()[offset + 1] & 0xFF);
    }

    private static int readUnsignedInt32(Cursor cursor) {
        cursor.requireAvailable(4, "uint32 payload");
        int offset = cursor.position();
        cursor.skip(4);
        return ((cursor.bytes()[offset] & 0xFF) << 24)
                | ((cursor.bytes()[offset + 1] & 0xFF) << 16)
                | ((cursor.bytes()[offset + 2] & 0xFF) << 8)
                | (cursor.bytes()[offset + 3] & 0xFF);
    }

    private static long readUnsignedInt64(Cursor cursor) {
        cursor.requireAvailable(8, "uint64 payload");
        int offset = cursor.position();
        cursor.skip(8);
        return ((long) cursor.bytes()[offset] & 0xFF) << 56
                | (((long) cursor.bytes()[offset + 1] & 0xFF) << 48)
                | (((long) cursor.bytes()[offset + 2] & 0xFF) << 40)
                | (((long) cursor.bytes()[offset + 3] & 0xFF) << 32)
                | (((long) cursor.bytes()[offset + 4] & 0xFF) << 24)
                | (((long) cursor.bytes()[offset + 5] & 0xFF) << 16)
                | (((long) cursor.bytes()[offset + 6] & 0xFF) << 8)
                | ((long) cursor.bytes()[offset + 7] & 0xFF);
    }

    private static void writeUnsignedInt16(ByteArrayOutputStream out, int value) {
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void writeUnsignedInt32(ByteArrayOutputStream out, int value) {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void writeUnsignedInt64(ByteArrayOutputStream out, long value) {
        out.write((int) (value >>> 56) & 0xFF);
        out.write((int) (value >>> 48) & 0xFF);
        out.write((int) (value >>> 40) & 0xFF);
        out.write((int) (value >>> 32) & 0xFF);
        out.write((int) (value >>> 24) & 0xFF);
        out.write((int) (value >>> 16) & 0xFF);
        out.write((int) (value >>> 8) & 0xFF);
        out.write((int) value & 0xFF);
    }

    private static void writeU8(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
    }

    private static void requireFlag(int actual, int expected, DataType type) {
        if (actual != expected) {
            throw new IllegalArgumentException(
                    "Unexpected primary key type flag for "
                            + type
                            + ": expected "
                            + expected
                            + ", got "
                            + actual);
        }
    }

    private void requireFullRowArity(InternalRow row) {
        if (row.getFieldCount() != rowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "Row arity "
                            + row.getFieldCount()
                            + " does not match type field count "
                            + rowType.getFieldCount());
        }
    }

    private void requirePrefixCount(int primaryKeyFieldCount) {
        if (primaryKeyFieldCount < 0 || primaryKeyFieldCount > primaryKeyFields.length) {
            throw new IllegalArgumentException(
                    "Invalid primary key prefix field count: " + primaryKeyFieldCount);
        }
    }

    private static void requireSupported(DataField field) {
        switch (field.type().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported primary key type for field " + field.name() + ": " + field.type());
        }
    }

    private static final class Cursor {

        private final byte[] bytes;
        private int position;

        private Cursor(byte[] bytes) {
            this.bytes = bytes;
        }

        private byte[] bytes() {
            return bytes;
        }

        private int position() {
            return position;
        }

        private boolean hasRemaining() {
            return position < bytes.length;
        }

        private int readU8(String section) {
            requireAvailable(1, section);
            return bytes[position++] & 0xFF;
        }

        private void skip(int length) {
            position += length;
        }

        private void requireAvailable(int length, String section) {
            if (length < 0 || position + length > bytes.length) {
                throw new IllegalArgumentException("Invalid or truncated " + section);
            }
        }
    }
}
