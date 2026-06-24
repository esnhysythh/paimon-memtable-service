package org.qwh.pms.lookup.local;

/** Local cache materialization mode for one Paimon data file. */
public enum LocalCacheMode {
    VALUE_SST,
    POSITION_SST,
    LOCAL_PARQUET,
    POSITION_SST_WITH_LOCAL_PARQUET
}
