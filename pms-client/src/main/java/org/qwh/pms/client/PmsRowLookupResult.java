package org.qwh.pms.client;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawLookupResult;

import java.util.Objects;
import java.util.Optional;

public record PmsRowLookupResult(PmsStatus status, LookupResultType type, InternalRow row) {

    public PmsRowLookupResult {
        Objects.requireNonNull(status, "status must not be null");
        if (status == PmsStatus.OK) {
            Objects.requireNonNull(type, "type must not be null for OK lookup result");
            if (type == LookupResultType.HIT) {
                Objects.requireNonNull(row, "row must not be null for HIT lookup result");
            } else if (row != null) {
                throw new IllegalArgumentException("row must be null for " + type + " lookup result");
            }
        } else {
            if (type != null) {
                throw new IllegalArgumentException("type must be null for non-OK lookup result");
            }
            if (row != null) {
                throw new IllegalArgumentException("row must be null for non-OK lookup result");
            }
        }
    }

    public static PmsRowLookupResult fromRaw(
            RawLookupResult raw,
            PmsRowValueCodec valueCodec,
            RowType rowType) {
        Objects.requireNonNull(raw, "raw must not be null");
        Objects.requireNonNull(valueCodec, "valueCodec must not be null");
        Objects.requireNonNull(rowType, "rowType must not be null");
        if (raw.status() != PmsStatus.OK) {
            return failed(raw.status());
        }
        return switch (raw.type()) {
            case HIT -> hit(valueCodec.decode(rowType, raw.row()));
            case MISS -> miss();
            case DELETED -> deleted();
        };
    }

    public static PmsRowLookupResult hit(InternalRow row) {
        return new PmsRowLookupResult(PmsStatus.OK, LookupResultType.HIT, row);
    }

    public static PmsRowLookupResult miss() {
        return new PmsRowLookupResult(PmsStatus.OK, LookupResultType.MISS, null);
    }

    public static PmsRowLookupResult deleted() {
        return new PmsRowLookupResult(PmsStatus.OK, LookupResultType.DELETED, null);
    }

    public static PmsRowLookupResult failed(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("failed lookup result cannot use OK status");
        }
        return new PmsRowLookupResult(status, null, null);
    }

    public boolean found() {
        return status == PmsStatus.OK && type == LookupResultType.HIT;
    }

    public Optional<InternalRow> rowOptional() {
        return Optional.ofNullable(row);
    }
}
