package org.qwh.pms.lookup.direct.parquet;

/** Physical row ordinal range selected by Parquet predicate filtering. */
public record RowRangeCandidate(long fromInclusive, long toExclusive) {}
