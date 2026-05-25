package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsRowValueCodecTest {

    private final PmsRowValueCodec codec = new PmsRowValueCodec();

    @Test
    void roundTripAllJavaApiTypes() {
        RowType nestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "nested_i", DataTypes.INT()),
                        DataTypes.FIELD(2, "nested_s", DataTypes.STRING()));
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "bool_col", DataTypes.BOOLEAN()),
                        DataTypes.FIELD(2, "byte_col", DataTypes.TINYINT()),
                        DataTypes.FIELD(3, "short_col", DataTypes.SMALLINT()),
                        DataTypes.FIELD(4, "int_col", DataTypes.INT()),
                        DataTypes.FIELD(5, "long_col", DataTypes.BIGINT()),
                        DataTypes.FIELD(6, "float_col", DataTypes.FLOAT()),
                        DataTypes.FIELD(7, "double_col", DataTypes.DOUBLE()),
                        DataTypes.FIELD(8, "string_col", DataTypes.STRING()),
                        DataTypes.FIELD(9, "decimal_col", DataTypes.DECIMAL(20, 4)),
                        DataTypes.FIELD(10, "timestamp_col", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(11, "binary_col", DataTypes.BYTES()),
                        DataTypes.FIELD(12, "array_col", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD(13, "map_col", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(14, "row_col", nestedType));

        GenericRow nested = new GenericRow(2);
        nested.setField(0, 7);
        nested.setField(1, BinaryString.fromString("nested"));

        Map<BinaryString, Integer> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("a"), 1);
        map.put(BinaryString.fromString("b"), 2);

        GenericRow row = new GenericRow(14);
        row.setField(0, true);
        row.setField(1, (byte) 12);
        row.setField(2, (short) 1234);
        row.setField(3, 123456);
        row.setField(4, 9_876_543_210L);
        row.setField(5, 1.25f);
        row.setField(6, 9.5d);
        row.setField(7, BinaryString.fromString("hello"));
        row.setField(8, Decimal.fromBigDecimal(new BigDecimal("123456789.1234"), 20, 4));
        row.setField(9, Timestamp.fromEpochMillis(1_700_000_000_123L, 456_000));
        row.setField(10, new byte[] {1, 2, 3, 4});
        row.setField(11, new GenericArray(new int[] {1, 2, 3}));
        row.setField(12, new GenericMap(map));
        row.setField(13, nested);

        byte[] encoded = codec.encode(rowType, row, 0);
        InternalRow decoded = codec.decode(rowType, encoded);

        assertEquals(RowKind.INSERT, decoded.getRowKind());
        assertTrue(decoded.getBoolean(0));
        assertEquals((byte) 12, decoded.getByte(1));
        assertEquals((short) 1234, decoded.getShort(2));
        assertEquals(123456, decoded.getInt(3));
        assertEquals(9_876_543_210L, decoded.getLong(4));
        assertEquals(1.25f, decoded.getFloat(5));
        assertEquals(9.5d, decoded.getDouble(6));
        assertEquals("hello", decoded.getString(7).toString());
        assertEquals(new BigDecimal("123456789.1234"), decoded.getDecimal(8, 20, 4).toBigDecimal());
        assertEquals(Timestamp.fromEpochMillis(1_700_000_000_123L, 456_000), decoded.getTimestamp(9, 6));
        assertArrayEquals(new byte[] {1, 2, 3, 4}, decoded.getBinary(10));

        InternalArray decodedArray = decoded.getArray(11);
        assertEquals(3, decodedArray.size());
        assertEquals(1, decodedArray.getInt(0));
        assertEquals(2, decodedArray.getInt(1));
        assertEquals(3, decodedArray.getInt(2));

        InternalMap decodedMap = decoded.getMap(12);
        assertEquals(2, decodedMap.size());
        assertEquals(BinaryString.fromString("a"), decodedMap.keyArray().getString(0));
        assertEquals(1, decodedMap.valueArray().getInt(0));
        assertEquals(BinaryString.fromString("b"), decodedMap.keyArray().getString(1));
        assertEquals(2, decodedMap.valueArray().getInt(1));

        InternalRow decodedNested = decoded.getRow(13, 2);
        assertEquals(7, decodedNested.getInt(0));
        assertEquals("nested", decodedNested.getString(1).toString());
    }

    @Test
    void supportsNullEmptyAndProjection() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "empty_string", DataTypes.STRING()),
                        DataTypes.FIELD(3, "null_string", DataTypes.STRING()),
                        DataTypes.FIELD(4, "payload", DataTypes.BYTES()));
        GenericRow row = new GenericRow(4);
        row.setField(0, 10);
        row.setField(1, BinaryString.fromString(""));
        row.setField(2, null);
        row.setField(3, new byte[] {9, 8, 7});

        byte[] encoded = codec.encode(rowType, row, 0);
        RowValueView view = codec.parse(encoded);

        assertEquals(FieldLookup.Kind.NOT_NULL, view.lookup(2).kind());
        assertEquals(0, view.payloadSlice(view.lookup(2).notNullIndex()).length());
        assertEquals(FieldLookup.Kind.NULL, view.lookup(3).kind());

        InternalRow projected = codec.decodeProjected(rowType, encoded, new int[] {2, 4});
        assertEquals(2, projected.getFieldCount());
        assertEquals("", projected.getString(0).toString());
        assertArrayEquals(new byte[] {9, 8, 7}, projected.getBinary(1));

        InternalRow projectedByName = codec.decodeProjected(rowType, encoded, new String[] {"payload", "empty_string"});
        assertArrayEquals(new byte[] {9, 8, 7}, projectedByName.getBinary(0));
        assertEquals("", projectedByName.getString(1).toString());
    }

    @Test
    void rejectsDeleteRowKindsAndNormalizesUpdateAfterToInsert() {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT()));
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.encode(rowType, GenericRow.ofKind(RowKind.DELETE, 1), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.encode(rowType, GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1), 0));

        GenericRow updateAfter = GenericRow.ofKind(RowKind.UPDATE_AFTER, 2);
        byte[] encodedUpdateAfter = codec.encode(rowType, updateAfter, 0);
        assertEquals(RowKind.INSERT, codec.decode(rowType, encodedUpdateAfter).getRowKind());
    }

    @Test
    void usesLargeRowWhenFieldIdExceedsSmallLimit() {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(300, "wide_id", DataTypes.INT()));
        GenericRow row = GenericRow.of(42);

        RowValueView view = codec.parse(codec.encode(rowType, row, 0));

        assertTrue((view.flags() & 1) != 0);
        assertEquals(FieldLookup.Kind.NOT_NULL, view.lookup(300).kind());
        assertEquals(42, codec.decode(rowType, codec.encode(rowType, row, 0)).getInt(0));
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> codec.parse(new byte[] {1, 0, 0}));
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.parse(new byte[] {2, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void supportsEmptyAndAllNullRows() {
        RowType emptyType = new RowType(List.of());
        byte[] emptyEncoded = codec.encode(emptyType, new GenericRow(0), 0);
        assertEquals(10, emptyEncoded.length);
        assertEquals(0, codec.decode(emptyType, emptyEncoded).getFieldCount());

        RowType allNullType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "i", DataTypes.INT()),
                        DataTypes.FIELD(2, "s", DataTypes.STRING()));
        GenericRow allNull = new GenericRow(2);
        byte[] encoded = codec.encode(allNullType, allNull, 0);
        RowValueView view = codec.parse(encoded);
        assertEquals(FieldLookup.Kind.NULL, view.lookup(1).kind());
        assertEquals(FieldLookup.Kind.NULL, view.lookup(2).kind());
        assertEquals(0, view.payloadLength());
    }

    @Test
    void largeRowCanBeTriggeredByPayloadLength() {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(1, "big", DataTypes.BYTES()));
        byte[] payload = new byte[70_000];
        Arrays.fill(payload, (byte) 7);
        GenericRow row = GenericRow.of(payload);

        byte[] encoded = codec.encode(rowType, row, 0);
        RowValueView view = codec.parse(encoded);

        assertTrue((view.flags() & 1) != 0);
        assertEquals(70_000, view.payloadLength());
        assertArrayEquals(payload, codec.decode(rowType, encoded).getBinary(0));
    }

    @Test
    void strictValidationRejectsUnsortedFieldIdsButTrustedCanParse() {
        byte[] unsorted =
                new byte[] {
                    1, 0, 0, 0,
                    0, 0, 2, 0,
                    0, 0,
                    2, 1,
                    4, 0, 8, 0,
                    1, 0, 0, 0,
                    2, 0, 0, 0
                };

        assertThrows(IllegalArgumentException.class, () -> codec.parse(unsorted, ValidationMode.STRICT));
        RowValueView trusted = codec.parse(unsorted, ValidationMode.TRUSTED);
        assertEquals(8, trusted.payloadLength());
    }

    @Test
    void rejectsTrailingBytes() {
        byte[] valid = codec.encode(DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT())), GenericRow.of(1), 0);
        byte[] withTail = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class, () -> codec.parse(withTail));
    }

    @Test
    void goldenBytesForSmallMixedRowAreStable() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "empty", DataTypes.STRING()),
                        DataTypes.FIELD(3, "missing_value", DataTypes.STRING()));
        GenericRow row = new GenericRow(3);
        row.setField(0, 42);
        row.setField(1, BinaryString.fromString(""));
        row.setField(2, null);

        assertEquals("01000000000002000100010203040004002a000000", hex(codec.encode(rowType, row, 0)));
    }

    @Test
    void schemaEvolutionSupportsReorderRenameAddAndDropByFieldId() {
        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.STRING()),
                        DataTypes.FIELD(3, "dropped", DataTypes.BIGINT()));
        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(2, "renamed_b", DataTypes.STRING()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(4, "added", DataTypes.STRING()));
        GenericRow row = GenericRow.of(5, BinaryString.fromString("value"), 999L);

        InternalRow decoded = codec.decode(readType, codec.encode(writeType, row, 0));

        assertEquals("value", decoded.getString(0).toString());
        assertEquals(5, decoded.getInt(1));
        assertNull(((GenericRow) decoded).getField(2));
    }

    @Test
    void missingNonNullAndDefaultFieldsFailExplicitly() {
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT()));
        GenericRow row = GenericRow.of(42);
        byte[] encoded = codec.encode(writeType, row, 0);

        RowType readWithRequired =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "required", DataTypes.INT().notNull()));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(readWithRequired, encoded));

        RowType readWithDefault =
                new RowType(
                        List.of(
                                DataTypes.FIELD(1, "id", DataTypes.INT()),
                                new org.apache.paimon.types.DataField(
                                        2, "with_default", DataTypes.INT(), null, "1")));
        assertThrows(UnsupportedOperationException.class, () -> codec.decode(readWithDefault, encoded));
    }

    @Test
    void missingNullableFieldDecodesAsNull() {
        RowType writeType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT()));
        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "added", DataTypes.STRING()));

        GenericRow row = GenericRow.of(42);
        InternalRow decoded = codec.decode(readType, codec.encode(writeType, row, 0));

        assertEquals(42, decoded.getInt(0));
        assertNull(((GenericRow) decoded).getField(1));
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(String.format("%02x", b & 0xFF));
        }
        return builder.toString();
    }
}
