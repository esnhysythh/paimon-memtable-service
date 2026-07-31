package org.qwh.pms.flink;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Connector 在 planning 和 runtime 之间传递的固定物理 schema. */
public final class PmsFlinkTableSchema implements Serializable {

    private final RowType rowType;
    private final List<String> primaryKeyNames;
    private final int[] primaryKeyIndexes;

    private PmsFlinkTableSchema(
            RowType rowType, List<String> primaryKeyNames, int[] primaryKeyIndexes) {
        this.rowType = rowType;
        this.primaryKeyNames = List.copyOf(primaryKeyNames);
        this.primaryKeyIndexes = primaryKeyIndexes.clone();
    }

    public static PmsFlinkTableSchema from(ResolvedCatalogTable table) {
        ResolvedSchema schema = table.getResolvedSchema();
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                throw new ValidationException(
                        "PMS Connector 只支持物理列, 不支持 computed 或 metadata column: "
                                + column.getName());
            }
        }
        if (table.isPartitioned()) {
            throw new ValidationException(
                    "PMS Connector 不接受 PARTITIONED BY, 分区与 bucket 路由由 PMS Server 绑定表决定.");
        }

        UniqueConstraint primaryKey =
                schema.getPrimaryKey()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "PMS Connector 要求声明 PRIMARY KEY ... NOT ENFORCED."));
        RowType rowType =
                (RowType) schema.toPhysicalRowDataType().getLogicalType();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        List<String> primaryKeyNames = primaryKey.getColumns();
        for (int primaryKeyIndex : primaryKeyIndexes) {
            RowType.RowField field = rowType.getFields().get(primaryKeyIndex);
            if (field.getType().isNullable()) {
                throw new ValidationException(
                        "PMS primary key 字段必须为 NOT NULL: " + field.getName());
            }
        }

        PmsFlinkTypeAdapter.validateTableTypes(rowType, primaryKeyIndexes);
        return new PmsFlinkTableSchema(rowType, primaryKeyNames, primaryKeyIndexes);
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> primaryKeyNames() {
        return primaryKeyNames;
    }

    public int[] primaryKeyIndexes() {
        return primaryKeyIndexes.clone();
    }

    public RowType primaryKeyRowType() {
        List<RowType.RowField> fields =
                Arrays.stream(primaryKeyIndexes)
                        .mapToObj(rowType.getFields()::get)
                        .toList();
        return new RowType(false, fields);
    }

    public List<LogicalType> primaryKeyTypes() {
        return Arrays.stream(primaryKeyIndexes)
                .mapToObj(rowType::getTypeAt)
                .toList();
    }
}
