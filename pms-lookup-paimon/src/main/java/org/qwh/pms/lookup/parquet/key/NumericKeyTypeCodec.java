package org.qwh.pms.lookup.parquet.key;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;

/** Supports Paimon numeric key roots that map to Parquet primitive numeric predicates. */
final class NumericKeyTypeCodec implements KeyTypeCodec {

    @Override
    public boolean supports(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        return root == DataTypeRoot.INTEGER
                || root == DataTypeRoot.BIGINT
                || root == DataTypeRoot.DATE;
    }
}
