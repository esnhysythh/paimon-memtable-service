package org.qwh.pms.lookup.key;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.TimestampType;

/** Supports timestamp keys that Paimon writes as Parquet INT64 millis/micros. */
final class TimestampInt64KeyTypeCodec implements KeyTypeCodec {

    @Override
    public boolean supports(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        return isTimestamp(root) && precision(type) <= 6;
    }

    private static int precision(DataType type) {
        if (type instanceof TimestampType timestampType) {
            return timestampType.getPrecision();
        }
        throw new UnsupportedOperationException("Unsupported timestamp key type: " + type);
    }

    private static boolean isTimestamp(DataTypeRoot root) {
        return root == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
    }
}
