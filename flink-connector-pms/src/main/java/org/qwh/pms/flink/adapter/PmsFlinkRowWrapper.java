package org.qwh.pms.flink.adapter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

/**
 * 将 Flink 1.20 RowData 只读适配为 PMS codec 使用的 Paimon InternalRow.
 *
 * <p>实现参考 Paimon 的 FlinkRowWrapper, 但只保留 PMS 已声明支持的类型. 这样不会把
 * Paimon 新版本为 Flink 1.x 回填的 Variant API 带入 Flink 1.20 classpath.
 */
public final class PmsFlinkRowWrapper implements InternalRow {

    private final RowData row;

    public PmsFlinkRowWrapper(RowData row) {
        this.row = row;
    }

    @Override
    public int getFieldCount() {
        return row.getArity();
    }

    @Override
    public org.apache.paimon.types.RowKind getRowKind() {
        return fromFlinkRowKind(row.getRowKind());
    }

    @Override
    public void setRowKind(org.apache.paimon.types.RowKind kind) {
        row.setRowKind(toFlinkRowKind(kind));
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
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(row.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        DecimalData value = row.getDecimal(pos, precision, scale);
        return Decimal.fromBigDecimal(value.toBigDecimal(), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        TimestampData value = row.getTimestamp(pos, precision);
        return Timestamp.fromEpochMillis(
                value.getMillisecond(), value.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkArrayWrapper(row.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new FlinkMapWrapper(row.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new PmsFlinkRowWrapper(row.getRow(pos, numFields));
    }

    @Override
    public Variant getVariant(int pos) {
        throw unsupported("VARIANT");
    }

    @Override
    public Blob getBlob(int pos) {
        throw unsupported("BLOB");
    }

    @Override
    public InternalVector getVector(int pos) {
        throw unsupported("VECTOR");
    }

    static org.apache.flink.types.RowKind toFlinkRowKind(
            org.apache.paimon.types.RowKind kind) {
        return switch (kind) {
            case INSERT -> org.apache.flink.types.RowKind.INSERT;
            case UPDATE_BEFORE -> org.apache.flink.types.RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER -> org.apache.flink.types.RowKind.UPDATE_AFTER;
            case DELETE -> org.apache.flink.types.RowKind.DELETE;
        };
    }

    static org.apache.paimon.types.RowKind fromFlinkRowKind(
            org.apache.flink.types.RowKind kind) {
        return switch (kind) {
            case INSERT -> org.apache.paimon.types.RowKind.INSERT;
            case UPDATE_BEFORE -> org.apache.paimon.types.RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER -> org.apache.paimon.types.RowKind.UPDATE_AFTER;
            case DELETE -> org.apache.paimon.types.RowKind.DELETE;
        };
    }

    private static UnsupportedOperationException unsupported(String type) {
        return new UnsupportedOperationException(
                "PMS Flink Connector 不支持 " + type + " row value.");
    }

    private static final class FlinkArrayWrapper implements InternalArray {

        private final ArrayData array;

        private FlinkArrayWrapper(ArrayData array) {
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
        public BinaryString getString(int pos) {
            return BinaryString.fromBytes(array.getString(pos).toBytes());
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            DecimalData value = array.getDecimal(pos, precision, scale);
            return Decimal.fromBigDecimal(value.toBigDecimal(), precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            TimestampData value = array.getTimestamp(pos, precision);
            return Timestamp.fromEpochMillis(
                    value.getMillisecond(), value.getNanoOfMillisecond());
        }

        @Override
        public byte[] getBinary(int pos) {
            return array.getBinary(pos);
        }

        @Override
        public InternalArray getArray(int pos) {
            return new FlinkArrayWrapper(array.getArray(pos));
        }

        @Override
        public InternalMap getMap(int pos) {
            return new FlinkMapWrapper(array.getMap(pos));
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return new PmsFlinkRowWrapper(array.getRow(pos, numFields));
        }

        @Override
        public Variant getVariant(int pos) {
            throw unsupported("VARIANT");
        }

        @Override
        public Blob getBlob(int pos) {
            throw unsupported("BLOB");
        }

        @Override
        public InternalVector getVector(int pos) {
            throw unsupported("VECTOR");
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

    private static final class FlinkMapWrapper implements InternalMap {

        private final MapData map;

        private FlinkMapWrapper(MapData map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public InternalArray keyArray() {
            return new FlinkArrayWrapper(map.keyArray());
        }

        @Override
        public InternalArray valueArray() {
            return new FlinkArrayWrapper(map.valueArray());
        }
    }
}
