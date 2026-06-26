package org.qwh.pms.lookup.parquet;

/** Physical row ordinal range selected by Parquet predicate filtering. */
public record RowRangeCandidate(long fromInclusive, long toExclusive) {}
