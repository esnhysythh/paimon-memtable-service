package org.qwh.pms.lookup.parquet.key;

import org.apache.paimon.types.DataType;

/** Describes one group of Paimon key types supported by direct Parquet lookup. */
interface KeyTypeCodec {

    boolean supports(DataType type);
}
