package org.qwh.pms.lookup.parquet.key;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;

/** Supports CHAR/VARCHAR/STRING keys through Parquet binary predicates. */
final class StringKeyTypeCodec implements KeyTypeCodec {

    @Override
    public boolean supports(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        // Paimon DataTypes.STRING() is represented as VARCHAR at the DataTypeRoot layer.
        return root == DataTypeRoot.CHAR || root == DataTypeRoot.VARCHAR;
    }
}
