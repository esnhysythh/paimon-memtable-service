package org.qwh.pms.flink.adapter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;

/** 将 PMS Lookup 解码得到的 Paimon InternalRow 只读适配为 Flink 1.20 RowData. */
public final class PmsFlinkRowData implements RowData {

    private final InternalRow row;

    public PmsFlinkRowData(InternalRow row) {
        this.row = row;
    }

    @Override
    public int getArity() {
        return row.getFieldCount();
    }

    @Override
    public org.apache.flink.types.RowKind getRowKind() {
        return PmsFlinkRowWrapper.toFlinkRowKind(row.getRowKind());
    }

    @Override
    public void setRowKind(org.apache.flink.types.RowKind kind) {
        row.setRowKind(PmsFlinkRowWrapper.fromFlinkRowKind(kind));
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromBytes(row.getString(pos).toBytes());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Decimal value = row.getDecimal(pos, precision, scale);
        return DecimalData.fromBigDecimal(value.toBigDecimal(), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        Timestamp value = row.getTimestamp(pos, precision);
        return TimestampData.fromEpochMillis(
                value.getMillisecond(), value.getNanoOfMillisecond());
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException(
                "PMS Flink Connector 不支持 RAW row value.");
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        return new PaimonArrayData(row.getArray(pos));
    }

    @Override
    public MapData getMap(int pos) {
        return new PaimonMapData(row.getMap(pos));
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return new PmsFlinkRowData(row.getRow(pos, numFields));
    }

    private static final class PaimonArrayData implements ArrayData {

        private final InternalArray array;

        private PaimonArrayData(InternalArray array) {
            this.array = array;
        }

        @Override
        public int size() {
            return array.size();
        }

        @Override
        public boolean isNullAt(int pos) {
            return array.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return array.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return array.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return array.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return array.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return array.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return array.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return array.getDouble(pos);
        }

        @Override
        public StringData getString(int pos) {
            return StringData.fromBytes(array.getString(pos).toBytes());
        }

        @Override
        public DecimalData getDecimal(int pos, int precision, int scale) {
            Decimal value = array.getDecimal(pos, precision, scale);
            return DecimalData.fromBigDecimal(value.toBigDecimal(), precision, scale);
        }

        @Override
        public TimestampData getTimestamp(int pos, int precision) {
            Timestamp value = array.getTimestamp(pos, precision);
            return TimestampData.fromEpochMillis(
                    value.getMillisecond(), value.getNanoOfMillisecond());
        }

        @Override
        public <T> RawValueData<T> getRawValue(int pos) {
            throw new UnsupportedOperationException(
                    "PMS Flink Connector 不支持 RAW array value.");
        }

        @Override
        public byte[] getBinary(int pos) {
            return array.getBinary(pos);
        }

        @Override
        public ArrayData getArray(int pos) {
            return new PaimonArrayData(array.getArray(pos));
        }

        @Override
        public MapData getMap(int pos) {
            return new PaimonMapData(array.getMap(pos));
        }

        @Override
        public RowData getRow(int pos, int numFields) {
            return new PmsFlinkRowData(array.getRow(pos, numFields));
        }

        @Override
        public boolean[] toBooleanArray() {
            return array.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return array.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return array.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return array.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return array.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return array.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return array.toDoubleArray();
        }
    }

    private static final class PaimonMapData implements MapData {

        private final InternalMap map;

        private PaimonMapData(InternalMap map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public ArrayData keyArray() {
            return new PaimonArrayData(map.keyArray());
        }

        @Override
        public ArrayData valueArray() {
            return new PaimonArrayData(map.valueArray());
        }
    }
}
