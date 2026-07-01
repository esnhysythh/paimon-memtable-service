package org.qwh.pms.protocol.api;

import java.util.List;
import java.util.Objects;

public record RawLookupBatchResult(PmsStatus status, List<RawLookupResult> results) {

    public RawLookupBatchResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(results, "results must not be null");
        for (RawLookupResult result : results) {
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

    public static RawLookupBatchResult ok(List<RawLookupResult> results) {
        return new RawLookupBatchResult(PmsStatus.OK, results);
    }

    public static RawLookupBatchResult single(RawLookupResult result) {
        Objects.requireNonNull(result, "result must not be null");
        return result.status() == PmsStatus.OK
                ? ok(List.of(result))
                : failed(result.status());
    }

    public static RawLookupBatchResult failed(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("failed lookup batch cannot use OK status");
        }
        return new RawLookupBatchResult(status, List.of());
    }
}
