package org.qwh.pms.flink.adapter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.codec.PmsRowValueCodec;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsFlinkRowAdapterTest {

    @Test
    void roundTripsEverySupportedRowValueFamily() {
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
                        DataTypes.FIELD(8, "char_col", DataTypes.CHAR(4)),
                        DataTypes.FIELD(9, "string_col", DataTypes.STRING()),
                        DataTypes.FIELD(10, "decimal_col", DataTypes.DECIMAL(20, 4)),
                        DataTypes.FIELD(11, "date_col", DataTypes.DATE()),
                        DataTypes.FIELD(12, "time_col", DataTypes.TIME(3)),
                        DataTypes.FIELD(13, "timestamp_col", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(
                                14,
                                "timestamp_ltz_col",
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                        DataTypes.FIELD(15, "binary_col", DataTypes.BINARY(4)),
                        DataTypes.FIELD(16, "bytes_col", DataTypes.BYTES()),
                        DataTypes.FIELD(17, "array_col", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD(
                                18,
                                "map_col",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(19, "row_col", nestedType),
                        DataTypes.FIELD(20, "null_col", DataTypes.STRING()));

        TimestampData timestamp =
                TimestampData.fromEpochMillis(1_700_000_000_123L, 456_000);
        GenericRowData nested =
                GenericRowData.of(7, StringData.fromString("nested"));
        Map<StringData, Integer> map = new LinkedHashMap<>();
        map.put(StringData.fromString("a"), 1);
        map.put(StringData.fromString("b"), 2);

        GenericRowData input = new GenericRowData(20);
        input.setField(0, true);
        input.setField(1, (byte) 12);
        input.setField(2, (short) 1234);
        input.setField(3, 123456);
        input.setField(4, 9_876_543_210L);
        input.setField(5, 1.25f);
        input.setField(6, 9.5d);
        input.setField(7, StringData.fromString("char"));
        input.setField(8, StringData.fromString("hello"));
        input.setField(
                9,
                DecimalData.fromBigDecimal(
                        new BigDecimal("123456789.1234"), 20, 4));
        input.setField(10, 19_000);
        input.setField(11, 42_123);
        input.setField(12, timestamp);
        input.setField(13, timestamp);
        input.setField(14, new byte[] {1, 2, 3, 4});
        input.setField(15, new byte[] {5, 6, 7});
        input.setField(16, new GenericArrayData(new Integer[] {1, null, 3}));
        input.setField(17, new GenericMapData(map));
        input.setField(18, nested);
        input.setField(19, null);

        PmsRowValueCodec codec = new PmsRowValueCodec();
        byte[] encoded = codec.encode(rowType, new PmsFlinkRowWrapper(input), 0);
        InternalRow decoded = codec.decode(rowType, encoded);
        RowData output = new PmsFlinkRowData(decoded);

        assertTrue(output.getBoolean(0));
        assertEquals((byte) 12, output.getByte(1));
        assertEquals((short) 1234, output.getShort(2));
        assertEquals(123456, output.getInt(3));
        assertEquals(9_876_543_210L, output.getLong(4));
        assertEquals(1.25f, output.getFloat(5));
        assertEquals(9.5d, output.getDouble(6));
        assertEquals("char", output.getString(7).toString());
        assertEquals("hello", output.getString(8).toString());
        assertEquals(
                new BigDecimal("123456789.1234"),
                output.getDecimal(9, 20, 4).toBigDecimal());
        assertEquals(19_000, output.getInt(10));
        assertEquals(42_123, output.getInt(11));
        assertEquals(timestamp, output.getTimestamp(12, 6));
        assertEquals(timestamp, output.getTimestamp(13, 6));
        assertArrayEquals(new byte[] {1, 2, 3, 4}, output.getBinary(14));
        assertArrayEquals(new byte[] {5, 6, 7}, output.getBinary(15));

        ArrayData array = output.getArray(16);
        assertEquals(3, array.size());
        assertEquals(1, array.getInt(0));
        assertTrue(array.isNullAt(1));
        assertEquals(3, array.getInt(2));

        MapData outputMap = output.getMap(17);
        assertEquals(2, outputMap.size());
        assertEquals("a", outputMap.keyArray().getString(0).toString());
        assertEquals(1, outputMap.valueArray().getInt(0));
        assertEquals("b", outputMap.keyArray().getString(1).toString());
        assertEquals(2, outputMap.valueArray().getInt(1));

        RowData outputNested = output.getRow(18, 2);
        assertEquals(7, outputNested.getInt(0));
        assertEquals("nested", outputNested.getString(1).toString());
        assertTrue(output.isNullAt(19));
        assertFalse(output.isNullAt(18));
        assertNull(input.getField(19));
    }
}
