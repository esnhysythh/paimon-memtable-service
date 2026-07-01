package org.qwh.pms.client;

import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawLookupBatchResult;

import java.util.List;
import java.util.Objects;

public record PmsRowLookupBatchResult(PmsStatus status, List<PmsRowLookupResult> results) {

    public PmsRowLookupBatchResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(results, "results must not be null");
        for (PmsRowLookupResult result : results) {
            Objects.requireNonNull(result, "result must not be null");
            if (status == PmsStatus.OK && result.status() != PmsStatus.OK) {
                throw new IllegalArgumentException("OK lookup batch cannot contain failed lookup result");
            }
        }
        if (status != PmsStatus.OK && !results.isEmpty()) {
            throw new IllegalArgumentException("non-OK lookup batch must not contain results");
        }
        results = List.copyOf(results);
    }

    public static PmsRowLookupBatchResult fromRaw(
            RawLookupBatchResult raw,
            PmsRowValueCodec valueCodec,
            RowType rowType) {
        Objects.requireNonNull(raw, "raw must not be null");
        Objects.requireNonNull(valueCodec, "valueCodec must not be null");
        Objects.requireNonNull(rowType, "rowType must not be null");
        if (raw.status() != PmsStatus.OK) {
            return failed(raw.status());
        }
        return ok(raw.results().stream()
            .map(result -> PmsRowLookupResult.fromRaw(result, valueCodec, rowType))
            .toList());
    }

    public static PmsRowLookupBatchResult ok(List<PmsRowLookupResult> results) {
        return new PmsRowLookupBatchResult(PmsStatus.OK, results);
    }

    public static PmsRowLookupBatchResult failed(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("failed lookup batch cannot use OK status");
        }
        return new PmsRowLookupBatchResult(status, List.of());
    }
}
