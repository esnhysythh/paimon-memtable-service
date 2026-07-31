package org.qwh.pms.flink.sink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * 提取完整 primary key, 供 Sink 前的 keyed exchange 使用.
 *
 * <p>Flink 可能复用输入 RowData, RowDataSerializer 也会复用内部 BinaryRowData. 因此返回前
 * 必须 copy, 否则后续记录会修改已经交给 network partitioner 的 key.
 */
final class PmsPrimaryKeySelector implements KeySelector<RowData, RowData> {

    private final int[] primaryKeyIndexes;
    private final RowType primaryKeyRowType;
    private transient RowData.FieldGetter[] fieldGetters;
    private transient RowDataSerializer serializer;

    PmsPrimaryKeySelector(
            RowType tableRowType, int[] primaryKeyIndexes, RowType primaryKeyRowType) {
        this.primaryKeyIndexes = primaryKeyIndexes.clone();
        this.primaryKeyRowType = primaryKeyRowType;
        initialize(tableRowType);
    }

    @Override
    public RowData getKey(RowData row) {
        ensureInitialized();
        GenericRowData key = new GenericRowData(primaryKeyIndexes.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            if (row.isNullAt(primaryKeyIndexes[i])) {
                throw new IllegalArgumentException(
                        "PMS primary key 字段不能为 NULL, ordinal=" + primaryKeyIndexes[i]);
            }
            Object value = fieldGetters[i].getFieldOrNull(row);
            key.setField(i, value);
        }
        BinaryRowData binaryKey = serializer.toBinaryRow(key);
        return binaryKey.copy();
    }

    private void ensureInitialized() {
        if (fieldGetters == null) {
            List<LogicalType> tableTypes =
                    primaryKeyRowType.getChildren();
            fieldGetters = new RowData.FieldGetter[primaryKeyIndexes.length];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] = RowData.createFieldGetter(tableTypes.get(i), primaryKeyIndexes[i]);
            }
            serializer = new RowDataSerializer(primaryKeyRowType);
        }
    }

    private void initialize(RowType tableRowType) {
        fieldGetters = new RowData.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldGetters[i] =
                    RowData.createFieldGetter(
                            tableRowType.getTypeAt(primaryKeyIndexes[i]), primaryKeyIndexes[i]);
        }
        serializer = new RowDataSerializer(primaryKeyRowType);
    }
}
