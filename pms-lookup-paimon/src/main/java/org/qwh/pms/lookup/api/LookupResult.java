package org.qwh.pms.lookup.api;

import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

import java.util.Optional;

/** Result for one lookup path. */
public final class LookupResult {

    public enum Kind {
        MISS,
        HIT,
        DELETED,
        UNKNOWN
    }

    private static final LookupResult MISS = new LookupResult(Kind.MISS, null, -1L);
    private static final LookupResult UNKNOWN = new LookupResult(Kind.UNKNOWN, null, -1L);
    private static final LookupResult DELETED = new LookupResult(Kind.DELETED, null, -1L);

    private final Kind kind;
    @Nullable private final InternalRow row;
    private final long rowIndex;

    private LookupResult(Kind kind, @Nullable InternalRow row, long rowIndex) {
        this.kind = kind;
        this.row = row;
        this.rowIndex = rowIndex;
    }

    public static LookupResult miss() {
        return MISS;
    }

    public static LookupResult unknown() {
        return UNKNOWN;
    }

    public static LookupResult deleted() {
        return DELETED;
    }

    public static LookupResult deleted(long rowIndex) {
        return new LookupResult(Kind.DELETED, null, rowIndex);
    }

    public static LookupResult hit(InternalRow row, long rowIndex) {
        return new LookupResult(Kind.HIT, row, rowIndex);
    }

    public Kind kind() {
        return kind;
    }

    public Optional<InternalRow> row() {
        return Optional.ofNullable(row);
    }

    public long rowIndex() {
        return rowIndex;
    }
}
