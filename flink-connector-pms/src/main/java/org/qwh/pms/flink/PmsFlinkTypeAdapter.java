package org.qwh.pms.flink;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.paimon.types.DataTypeRoot;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/** Flink 类型、Paimon 类型和 PMS 当前 profile 之间的唯一校验入口. */
public final class PmsFlinkTypeAdapter {

    private PmsFlinkTypeAdapter() {}

    public static void validateTableTypes(RowType rowType, int[] primaryKeyIndexes) {
        for (RowType.RowField field : rowType.getFields()) {
            validateRowValueType(field.getType(), field.getName());
        }
        for (int primaryKeyIndex : primaryKeyIndexes) {
            RowType.RowField field = rowType.getFields().get(primaryKeyIndex);
            validatePrimaryKeyType(field.getType(), field.getName());
        }
    }

    public static void validateServerSchema(
            PmsFlinkTableSchema flinkSchema,
            org.apache.paimon.types.RowType serverRowType,
            List<String> serverPrimaryKeys) {
        validateCompatibleType(flinkSchema.rowType(), serverRowType, "$");
        if (!flinkSchema.primaryKeyNames().equals(serverPrimaryKeys)) {
            throw new ValidationException(
                    "Flink DDL 与 PMS Server primary key 顺序不一致. Flink="
                            + flinkSchema.primaryKeyNames()
                            + ", PMS="
                            + serverPrimaryKeys);
        }
    }

    public static Object toFlinkInternalLiteral(Object value, LogicalType type) {
        return switch (type.getTypeRoot()) {
            case INTEGER -> (Integer) value;
            case BIGINT -> (Long) value;
            case DATE -> Math.toIntExact(((LocalDate) value).toEpochDay());
            case CHAR, VARCHAR -> StringData.fromString((String) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE ->
                    TimestampData.fromLocalDateTime((LocalDateTime) value);
            default ->
                    throw new ValidationException(
                            "不支持将 DELETE literal 转换为 PMS primary key: " + type);
        };
    }

    private static void validateRowValueType(LogicalType type, String fieldPath) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return;
            case ARRAY:
                validateRowValueType(type.getChildren().get(0), fieldPath + "[]");
                return;
            case MAP:
                validateRowValueType(type.getChildren().get(0), fieldPath + "<key>");
                validateRowValueType(type.getChildren().get(1), fieldPath + "<value>");
                return;
            case ROW:
                RowType rowType = (RowType) type;
                for (RowType.RowField field : rowType.getFields()) {
                    validateRowValueType(field.getType(), fieldPath + "." + field.getName());
                }
                return;
            default:
                throw new ValidationException(
                        "PMS RowValueCodec 不支持字段类型: " + fieldPath + " " + type);
        }
    }

    private static void validatePrimaryKeyType(LogicalType type, String fieldName) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.INTEGER
                || root == LogicalTypeRoot.BIGINT
                || root == LogicalTypeRoot.DATE
                || root == LogicalTypeRoot.VARCHAR) {
            return;
        }
        if (root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                && ((TimestampType) type).getPrecision() <= 6) {
            return;
        }
        throw new ValidationException(
                "PMS Connector 当前 Lookup profile 不支持 primary key 类型: "
                        + fieldName
                        + " "
                        + type);
    }

    /**
     * 逐层比较 Flink 与 PMS/Paimon schema.
     *
     * <p>这里不调用 Paimon Flink Connector 的 LogicalTypeConversion. Paimon 1.4 为
     * Flink 1.x 回填了 Variant 类型, 直接依赖会向 Flink 1.20 注入同名 API. PMS
     * 不支持 Variant, 因此显式比较当前 profile 更简单也更安全.
     */
    private static void validateCompatibleType(
            LogicalType flinkType,
            org.apache.paimon.types.DataType paimonType,
            String fieldPath) {
        // Flink physical row container 固定为 NOT NULL, Paimon table RowType 根节点通常为
        // nullable. 根节点不是一个可写字段, 只比较其子字段的 nullability.
        boolean topLevelRow =
                "$".equals(fieldPath)
                        && flinkType.getTypeRoot() == LogicalTypeRoot.ROW
                        && paimonType.getTypeRoot() == DataTypeRoot.ROW;
        if (!topLevelRow
                && flinkType.isNullable() != paimonType.isNullable()) {
            mismatch(fieldPath, flinkType, paimonType);
        }
        switch (flinkType.getTypeRoot()) {
            case BOOLEAN -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.BOOLEAN);
            case TINYINT -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.TINYINT);
            case SMALLINT -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.SMALLINT);
            case INTEGER -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.INTEGER);
            case BIGINT -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.BIGINT);
            case FLOAT -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.FLOAT);
            case DOUBLE -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.DOUBLE);
            case DATE -> requireRoot(
                    fieldPath, flinkType, paimonType, DataTypeRoot.DATE);
            case CHAR -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.CHAR);
                if (((CharType) flinkType).getLength()
                        != ((org.apache.paimon.types.CharType) paimonType)
                                .getLength()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case VARCHAR -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.VARCHAR);
                if (((VarCharType) flinkType).getLength()
                        != ((org.apache.paimon.types.VarCharType) paimonType)
                                .getLength()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case BINARY -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.BINARY);
                if (((BinaryType) flinkType).getLength()
                        != ((org.apache.paimon.types.BinaryType) paimonType)
                                .getLength()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case VARBINARY -> {
                requireRoot(
                        fieldPath, flinkType, paimonType, DataTypeRoot.VARBINARY);
                if (((VarBinaryType) flinkType).getLength()
                        != ((org.apache.paimon.types.VarBinaryType) paimonType)
                                .getLength()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case DECIMAL -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.DECIMAL);
                DecimalType flinkDecimal = (DecimalType) flinkType;
                org.apache.paimon.types.DecimalType paimonDecimal =
                        (org.apache.paimon.types.DecimalType) paimonType;
                if (flinkDecimal.getPrecision() != paimonDecimal.getPrecision()
                        || flinkDecimal.getScale() != paimonDecimal.getScale()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case TIME_WITHOUT_TIME_ZONE -> {
                requireRoot(
                        fieldPath,
                        flinkType,
                        paimonType,
                        DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
                if (((TimeType) flinkType).getPrecision()
                        != ((org.apache.paimon.types.TimeType) paimonType)
                                .getPrecision()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE -> {
                requireRoot(
                        fieldPath,
                        flinkType,
                        paimonType,
                        DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
                if (((TimestampType) flinkType).getPrecision()
                        != ((org.apache.paimon.types.TimestampType) paimonType)
                                .getPrecision()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                requireRoot(
                        fieldPath,
                        flinkType,
                        paimonType,
                        DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                if (((LocalZonedTimestampType) flinkType).getPrecision()
                        != ((org.apache.paimon.types.LocalZonedTimestampType)
                                        paimonType)
                                .getPrecision()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
            }
            case ARRAY -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.ARRAY);
                validateCompatibleType(
                        ((ArrayType) flinkType).getElementType(),
                        ((org.apache.paimon.types.ArrayType) paimonType)
                                .getElementType(),
                        fieldPath + "[]");
            }
            case MAP -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.MAP);
                MapType flinkMap = (MapType) flinkType;
                org.apache.paimon.types.MapType paimonMap =
                        (org.apache.paimon.types.MapType) paimonType;
                validateCompatibleType(
                        flinkMap.getKeyType(),
                        paimonMap.getKeyType(),
                        fieldPath + "<key>");
                validateCompatibleType(
                        flinkMap.getValueType(),
                        paimonMap.getValueType(),
                        fieldPath + "<value>");
            }
            case ROW -> {
                requireRoot(fieldPath, flinkType, paimonType, DataTypeRoot.ROW);
                RowType flinkRow = (RowType) flinkType;
                org.apache.paimon.types.RowType paimonRow =
                        (org.apache.paimon.types.RowType) paimonType;
                if (flinkRow.getFieldCount() != paimonRow.getFieldCount()) {
                    mismatch(fieldPath, flinkType, paimonType);
                }
                for (int i = 0; i < flinkRow.getFieldCount(); i++) {
                    RowType.RowField flinkField = flinkRow.getFields().get(i);
                    org.apache.paimon.types.DataField paimonField =
                            paimonRow.getFields().get(i);
                    if (!flinkField.getName().equals(paimonField.name())) {
                        mismatch(fieldPath, flinkType, paimonType);
                    }
                    validateCompatibleType(
                            flinkField.getType(),
                            paimonField.type(),
                            fieldPath + "." + flinkField.getName());
                }
            }
            default -> mismatch(fieldPath, flinkType, paimonType);
        }
    }

    private static void requireRoot(
            String fieldPath,
            LogicalType flinkType,
            org.apache.paimon.types.DataType paimonType,
            DataTypeRoot expectedRoot) {
        if (paimonType.getTypeRoot() != expectedRoot) {
            mismatch(fieldPath, flinkType, paimonType);
        }
    }

    private static void mismatch(
            String fieldPath,
            LogicalType flinkType,
            org.apache.paimon.types.DataType paimonType) {
        throw new ValidationException(
                "Flink DDL 与 PMS Server schema 不一致: path="
                        + fieldPath
                        + ", Flink="
                        + flinkType.asSummaryString()
                        + ", PMS="
                        + paimonType.asSQLString());
    }
}
