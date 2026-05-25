package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsPrimaryKeyCodecTest {

    @Test
    void goldenBytesForMixedKeyAreStable() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()),
                        DataTypes.FIELD(3, "day", DataTypes.DATE()),
                        DataTypes.FIELD(4, "time", DataTypes.TIME()),
                        DataTypes.FIELD(5, "ts", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(6, "raw", DataTypes.BYTES()));
        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(rowType, new int[] {1, 2, 3, 4, 5, 6});
        GenericRow row =
                GenericRow.of(
                        -1,
                        BinaryString.fromString("abc"),
                        -1,
                        1,
                        Timestamp.fromEpochMillis(-1, 5),
                        new byte[] {1, 2, 3, 0});

        assertEquals(
                "057fffffff016162630000000000fa097fffffff0a00000001087fffffffffffffff00000005010102030000000000fb",
                hex(codec.encodeKey(row)));
    }

    @Test
    void decodeKeyRoundTripsSupportedTypesAsKeyTuple() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "tiny", DataTypes.TINYINT()),
                        DataTypes.FIELD(2, "small", DataTypes.SMALLINT()),
                        DataTypes.FIELD(3, "int", DataTypes.INT()),
                        DataTypes.FIELD(4, "long", DataTypes.BIGINT()),
                        DataTypes.FIELD(5, "date", DataTypes.DATE()),
                        DataTypes.FIELD(6, "time", DataTypes.TIME()),
                        DataTypes.FIELD(7, "string", DataTypes.STRING()),
                        DataTypes.FIELD(8, "binary", DataTypes.BYTES()),
                        DataTypes.FIELD(9, "ts3", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD(10, "ts6", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(11, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        PmsPrimaryKeyCodec codec =
                new PmsPrimaryKeyCodec(rowType, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
        GenericRow row =
                GenericRow.of(
                        (byte) -3,
                        (short) -300,
                        -100_000,
                        -10_000_000_000L,
                        -10,
                        12345,
                        BinaryString.fromBytes(new byte[] {'a', 0, (byte) 0xFF}),
                        new byte[] {1, 2, 3, 0, (byte) 0xFF},
                        Timestamp.fromEpochMillis(123),
                        Timestamp.fromEpochMillis(123, 456),
                        Timestamp.fromEpochMillis(-123, 789));

        InternalRow decoded = codec.decodeKey(codec.encodeKey(row));

        assertEquals(11, decoded.getFieldCount());
        assertEquals((byte) -3, decoded.getByte(0));
        assertEquals((short) -300, decoded.getShort(1));
        assertEquals(-100_000, decoded.getInt(2));
        assertEquals(-10_000_000_000L, decoded.getLong(3));
        assertEquals(-10, decoded.getInt(4));
        assertEquals(12345, decoded.getInt(5));
        assertEquals(BinaryString.fromBytes(new byte[] {'a', 0, (byte) 0xFF}), decoded.getString(6));
        assertArrayEquals(new byte[] {1, 2, 3, 0, (byte) 0xFF}, decoded.getBinary(7));
        assertEquals(Timestamp.fromEpochMillis(123), decoded.getTimestamp(8, 3));
        assertEquals(Timestamp.fromEpochMillis(123, 456), decoded.getTimestamp(9, 6));
        assertEquals(Timestamp.fromEpochMillis(-123, 789), decoded.getTimestamp(10, 6));
    }

    @Test
    void decodePrefixReturnsOnlyRequestedKeyTupleFields() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()),
                        DataTypes.FIELD(3, "ts", DataTypes.TIMESTAMP(6)));
        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(rowType, new int[] {1, 2, 3});
        GenericRow row = GenericRow.of(42, BinaryString.fromString("value"), Timestamp.fromEpochMillis(10, 20));

        byte[] prefix = codec.encodePrefix(row, 2);
        InternalRow decoded = codec.decodePrefix(prefix, 2);

        assertEquals(2, decoded.getFieldCount());
        assertEquals(42, decoded.getInt(0));
        assertEquals("value", decoded.getString(1).toString());
    }

    @Test
    void integerDateAndTimeOrderingMatchesLogicalOrdering() {
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.TINYINT())),
                new int[] {1},
                List.of(
                        GenericRow.of(Byte.MIN_VALUE),
                        GenericRow.of((byte) -1),
                        GenericRow.of((byte) 0),
                        GenericRow.of((byte) 1),
                        GenericRow.of(Byte.MAX_VALUE)));
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.SMALLINT())),
                new int[] {1},
                List.of(
                        GenericRow.of(Short.MIN_VALUE),
                        GenericRow.of((short) -1),
                        GenericRow.of((short) 0),
                        GenericRow.of((short) 1),
                        GenericRow.of(Short.MAX_VALUE)));
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.INT())),
                new int[] {1},
                List.of(
                        GenericRow.of(Integer.MIN_VALUE),
                        GenericRow.of(-1),
                        GenericRow.of(0),
                        GenericRow.of(1),
                        GenericRow.of(Integer.MAX_VALUE)));
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.BIGINT())),
                new int[] {1},
                List.of(
                        GenericRow.of(Long.MIN_VALUE),
                        GenericRow.of(-1L),
                        GenericRow.of(0L),
                        GenericRow.of(1L),
                        GenericRow.of(Long.MAX_VALUE)));
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.DATE())),
                new int[] {1},
                List.of(GenericRow.of(-1), GenericRow.of(0), GenericRow.of(1)));
        assertEncodedOrder(
                DataTypes.ROW(DataTypes.FIELD(1, "v", DataTypes.TIME())),
                new int[] {1},
                List.of(GenericRow.of(0), GenericRow.of(1), GenericRow.of(86_399_999)));
    }

    @Test
    void stringAndBinaryOrderingMatchesUnsignedByteOrdering() {
        RowType stringType = DataTypes.ROW(DataTypes.FIELD(1, "s", DataTypes.STRING()));
        assertEncodedOrder(
                stringType,
                new int[] {1},
                List.of(
                        GenericRow.of(BinaryString.fromString("")),
                        GenericRow.of(BinaryString.fromString("a")),
                        GenericRow.of(BinaryString.fromBytes(new byte[] {'a', 0})),
                        GenericRow.of(BinaryString.fromString("aa")),
                        GenericRow.of(BinaryString.fromString("abcdefgh")),
                        GenericRow.of(BinaryString.fromString("abcdefghi"))));

        RowType binaryType = DataTypes.ROW(DataTypes.FIELD(1, "b", DataTypes.BYTES()));
        assertEncodedOrder(
                binaryType,
                new int[] {1},
                List.of(
                        GenericRow.of(new byte[] {}),
                        GenericRow.of(new byte[] {0}),
                        GenericRow.of(new byte[] {0, (byte) 0xFF}),
                        GenericRow.of(new byte[] {1}),
                        GenericRow.of(new byte[] {(byte) 0xFF})));

        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(binaryType, new int[] {1});
        assertEquals("010000000000000000f7", hex(codec.encodeKey(GenericRow.of(new byte[] {}))));
        assertEquals(
                "010102030405060708ff0000000000000000f7",
                hex(codec.encodeKey(GenericRow.of(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}))));
    }

    @Test
    void timestampEncodingUsesPrecisionSpecificWidthAndOrdering() {
        RowType millisType = DataTypes.ROW(DataTypes.FIELD(1, "ts", DataTypes.TIMESTAMP(3)));
        PmsPrimaryKeyCodec millisCodec = new PmsPrimaryKeyCodec(millisType, new int[] {1});
        assertEquals(
                "07800000000000000a",
                hex(millisCodec.encodeKey(GenericRow.of(Timestamp.fromEpochMillis(10)))));
        assertThrows(
                IllegalArgumentException.class,
                () -> millisCodec.encodeKey(GenericRow.of(Timestamp.fromEpochMillis(10, 1))));

        RowType nanosType = DataTypes.ROW(DataTypes.FIELD(1, "ts", DataTypes.TIMESTAMP(6)));
        PmsPrimaryKeyCodec nanosCodec = new PmsPrimaryKeyCodec(nanosType, new int[] {1});
        assertEquals(
                "08800000000000000a00000001",
                hex(nanosCodec.encodeKey(GenericRow.of(Timestamp.fromEpochMillis(10, 1)))));
        assertEncodedOrder(
                nanosType,
                new int[] {1},
                List.of(
                        GenericRow.of(Timestamp.fromEpochMillis(-1, 999_999)),
                        GenericRow.of(Timestamp.fromEpochMillis(0, 0)),
                        GenericRow.of(Timestamp.fromEpochMillis(0, 1)),
                        GenericRow.of(Timestamp.fromEpochMillis(1, 0))));
    }

    @Test
    void compositeKeyOrderingMatchesTupleOrdering() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()),
                        DataTypes.FIELD(3, "raw", DataTypes.BYTES()),
                        DataTypes.FIELD(4, "ts", DataTypes.TIMESTAMP(6)));
        int[] keyFields = {1, 2, 3, 4};
        List<InternalRow> rows =
                List.of(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("a"),
                                new byte[] {1},
                                Timestamp.fromEpochMillis(0, 0)),
                        GenericRow.of(
                                1,
                                BinaryString.fromString("a"),
                                new byte[] {1},
                                Timestamp.fromEpochMillis(0, 1)),
                        GenericRow.of(
                                1,
                                BinaryString.fromString("a"),
                                new byte[] {2},
                                Timestamp.fromEpochMillis(0, 0)),
                        GenericRow.of(
                                1,
                                BinaryString.fromString("aa"),
                                new byte[] {0},
                                Timestamp.fromEpochMillis(0, 0)),
                        GenericRow.of(
                                2,
                                BinaryString.fromString(""),
                                new byte[] {},
                                Timestamp.fromEpochMillis(-1, 0)),
                        GenericRow.of(
                                -1,
                                BinaryString.fromString("z"),
                                new byte[] {(byte) 0xFF},
                                Timestamp.fromEpochMillis(0, 0)));
        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(rowType, keyFields);

        for (InternalRow left : rows) {
            for (InternalRow right : rows) {
                int logical = compareComposite(left, right);
                int encoded = unsignedCompare(codec.encodeKey(left), codec.encodeKey(right));
                assertEquals(Integer.signum(logical), Integer.signum(encoded));
            }
        }
    }

    @Test
    void prefixRangeCoversContiguousCompositeKeys() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()));
        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(rowType, new int[] {1, 2});
        List<InternalRow> rows =
                List.of(
                        GenericRow.of(2, BinaryString.fromString("a")),
                        GenericRow.of(1, BinaryString.fromString("b")),
                        GenericRow.of(0, BinaryString.fromString("z")),
                        GenericRow.of(1, BinaryString.fromString("a")),
                        GenericRow.of(1, BinaryString.fromString("c")));
        List<byte[]> sortedKeys = new ArrayList<>();
        for (InternalRow row : rows) {
            sortedKeys.add(codec.encodeKey(row));
        }
        sortedKeys.sort(PmsPrimaryKeyCodecTest::unsignedCompare);

        byte[] prefix = codec.encodePrefix(GenericRow.of(1, BinaryString.fromString("ignored")), 1);
        Optional<byte[]> upper = PmsPrimaryKeyCodec.prefixNext(prefix);
        assertTrue(upper.isPresent());

        List<byte[]> inRange = new ArrayList<>();
        for (byte[] key : sortedKeys) {
            if (unsignedCompare(key, prefix) >= 0 && unsignedCompare(key, upper.get()) < 0) {
                inRange.add(key);
            }
        }

        assertEquals(3, inRange.size());
        assertEquals(sortedKeys.subList(1, 4), inRange);
    }

    @Test
    void schemaReorderAndRenameKeepFieldIdBasedKeyStable() {
        RowType original =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()));
        RowType reordered =
                DataTypes.ROW(
                        DataTypes.FIELD(2, "renamed_name", DataTypes.STRING()),
                        DataTypes.FIELD(1, "renamed_id", DataTypes.INT()));

        byte[] originalKey =
                new PmsPrimaryKeyCodec(original, new int[] {1, 2})
                        .encodeKey(GenericRow.of(42, BinaryString.fromString("value")));
        byte[] reorderedKey =
                new PmsPrimaryKeyCodec(reordered, new int[] {1, 2})
                        .encodeKey(GenericRow.of(BinaryString.fromString("value"), 42));

        assertArrayEquals(originalKey, reorderedKey);
    }

    @Test
    void tupleEncodingAndPrefixNextWorkWithoutFullRow() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "name", DataTypes.STRING()));
        PmsPrimaryKeyCodec codec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id", "name"));

        assertArrayEquals(
                codec.encodeKey(GenericRow.of(7, BinaryString.fromString("a"))),
                codec.encodeKeyTuple(GenericRow.of(7, BinaryString.fromString("a"))));
        assertArrayEquals(
                codec.encodePrefix(GenericRow.of(7, BinaryString.fromString("ignored")), 1),
                codec.encodePrefixTuple(GenericRow.of(7)));
        assertFalse(PmsPrimaryKeyCodec.prefixNext(new byte[] {(byte) 0xFF}).isPresent());
    }

    @Test
    void rejectsNullUnsupportedAndInvalidValues() {
        RowType intType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT()));
        PmsPrimaryKeyCodec intCodec = new PmsPrimaryKeyCodec(intType, new int[] {1});
        assertThrows(IllegalArgumentException.class, () -> intCodec.encodeKey(GenericRow.of((Object) null)));

        RowType boolType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.BOOLEAN()));
        assertThrows(UnsupportedOperationException.class, () -> new PmsPrimaryKeyCodec(boolType, new int[] {1}));

        RowType timeType = DataTypes.ROW(DataTypes.FIELD(1, "t", DataTypes.TIME()));
        PmsPrimaryKeyCodec timeCodec = new PmsPrimaryKeyCodec(timeType, new int[] {1});
        assertThrows(IllegalArgumentException.class, () -> timeCodec.encodeKey(GenericRow.of(-1)));
        assertThrows(IllegalArgumentException.class, () -> timeCodec.encodeKey(GenericRow.of(86_400_000)));

        assertThrows(IllegalArgumentException.class, () -> intCodec.encodePrefix(GenericRow.of(1), 2));
        assertThrows(IllegalArgumentException.class, () -> intCodec.encodeKeyTuple(GenericRow.of(1, 2)));
    }

    @Test
    void decodeRejectsCorruptOrMismatchedKeys() {
        RowType intType = DataTypes.ROW(DataTypes.FIELD(1, "id", DataTypes.INT()));
        PmsPrimaryKeyCodec intCodec = new PmsPrimaryKeyCodec(intType, new int[] {1});
        byte[] validInt = intCodec.encodeKey(GenericRow.of(1));

        assertThrows(IllegalArgumentException.class, () -> intCodec.decodeKey(new byte[] {0x05}));
        assertThrows(IllegalArgumentException.class, () -> intCodec.decodeKey(new byte[] {0x01}));
        assertThrows(
                IllegalArgumentException.class,
                () -> intCodec.decodeKey(Arrays.copyOf(validInt, validInt.length + 1)));

        RowType bytesType = DataTypes.ROW(DataTypes.FIELD(1, "raw", DataTypes.BYTES()));
        PmsPrimaryKeyCodec bytesCodec = new PmsPrimaryKeyCodec(bytesType, new int[] {1});
        assertThrows(
                IllegalArgumentException.class,
                () -> bytesCodec.decodeKey(
                        new byte[] {0x01, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF6}));
        assertThrows(
                IllegalArgumentException.class,
                () -> bytesCodec.decodeKey(
                        new byte[] {0x01, 1, 2, 3, 0, 1, 0, 0, 0, (byte) 0xFA}));
        assertThrows(
                IllegalArgumentException.class,
                () -> bytesCodec.decodeKey(
                        new byte[] {0x01, 1, 2, 3, 4, 5, 6, 7, 8, (byte) 0xFF}));

        RowType tsType = DataTypes.ROW(DataTypes.FIELD(1, "ts", DataTypes.TIMESTAMP(6)));
        PmsPrimaryKeyCodec tsCodec = new PmsPrimaryKeyCodec(tsType, new int[] {1});
        assertThrows(
                IllegalArgumentException.class,
                () -> tsCodec.decodeKey(
                        new byte[] {
                            0x08,
                            (byte) 0x80,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0x0F,
                            0x42,
                            0x40
                        }));
    }

    private static void assertEncodedOrder(RowType rowType, int[] keyFields, List<InternalRow> rows) {
        PmsPrimaryKeyCodec codec = new PmsPrimaryKeyCodec(rowType, keyFields);
        for (int i = 1; i < rows.size(); i++) {
            assertTrue(
                    unsignedCompare(codec.encodeKey(rows.get(i - 1)), codec.encodeKey(rows.get(i))) < 0,
                    "Expected encoded row " + (i - 1) + " to sort before row " + i);
        }
    }

    private static int compareComposite(InternalRow left, InternalRow right) {
        int cmp = Integer.compare(left.getInt(0), right.getInt(0));
        if (cmp != 0) {
            return cmp;
        }
        cmp = left.getString(1).compareTo(right.getString(1));
        if (cmp != 0) {
            return cmp;
        }
        cmp = unsignedCompare(left.getBinary(2), right.getBinary(2));
        if (cmp != 0) {
            return cmp;
        }
        return left.getTimestamp(3, 6).compareTo(right.getTimestamp(3, 6));
    }

    private static int unsignedCompare(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int cmp = (left[i] & 0xFF) - (right[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(String.format("%02x", b & 0xFF));
        }
        return builder.toString();
    }
}
