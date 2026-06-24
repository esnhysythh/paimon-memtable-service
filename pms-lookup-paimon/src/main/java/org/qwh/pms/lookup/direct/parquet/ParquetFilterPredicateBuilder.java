package org.qwh.pms.lookup.direct.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.TimestampType;
import org.qwh.pms.lookup.key.LookupKeySpec;

import org.apache.paimon.shade.org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.paimon.shade.org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.paimon.shade.org.apache.parquet.io.api.Binary;

/** Builds Parquet equality predicates for direct lookup keys. */
public final class ParquetFilterPredicateBuilder {

    private ParquetFilterPredicateBuilder() {}

    public static FilterPredicate build(LookupKeySpec keySpec, InternalRow key) {
        FilterPredicate predicate = null;
        for (int keyOrdinal = 0; keyOrdinal < keySpec.keyFieldCount(); keyOrdinal++) {
            int tableFieldIndex = keySpec.keyFieldIndex(keyOrdinal);
            String fieldName = keySpec.tableRowType().getFieldNames().get(tableFieldIndex);
            DataType type = keySpec.keyType().getTypeAt(keyOrdinal);
            FilterPredicate equality = equality(fieldName, type, key, keyOrdinal);
            predicate = predicate == null ? equality : FilterApi.and(predicate, equality);
        }
        return predicate;
    }

    private static FilterPredicate equality(
            String fieldName, DataType type, InternalRow key, int keyOrdinal) {
        DataTypeRoot root = type.getTypeRoot();
        if (root == DataTypeRoot.INTEGER || root == DataTypeRoot.DATE) {
            return FilterApi.eq(FilterApi.intColumn(fieldName), key.getInt(keyOrdinal));
        }
        if (root == DataTypeRoot.BIGINT) {
            return FilterApi.eq(FilterApi.longColumn(fieldName), key.getLong(keyOrdinal));
        }
        if (root == DataTypeRoot.CHAR || root == DataTypeRoot.VARCHAR) {
            BinaryString value = key.getString(keyOrdinal);
            return FilterApi.eq(
                    FilterApi.binaryColumn(fieldName),
                    Binary.fromConstantByteArray(value.toBytes()));
        }
        if (isTimestamp(root)) {
            int precision = timestampPrecision(type);
            Timestamp value = key.getTimestamp(keyOrdinal, precision);
            long parquetValue = precision <= 3 ? value.getMillisecond() : value.toMicros();
            return FilterApi.eq(FilterApi.longColumn(fieldName), parquetValue);
        }
        throw new UnsupportedOperationException(
                "Unsupported direct seek key type for Parquet predicate: " + type);
    }

    private static int timestampPrecision(DataType type) {
        if (type instanceof TimestampType timestampType) {
            return timestampType.getPrecision();
        }
        throw new UnsupportedOperationException("Unsupported timestamp key type: " + type);
    }

    private static boolean isTimestamp(DataTypeRoot root) {
        return root == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
    }
}
