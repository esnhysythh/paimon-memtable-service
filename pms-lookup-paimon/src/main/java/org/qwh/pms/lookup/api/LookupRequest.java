package org.qwh.pms.lookup.api;

import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

import java.util.Optional;

/** Lookup request shared by direct Parquet seek and future local-index paths. */
public final class LookupRequest {

    private final InternalRow key;
    @Nullable private final int[] projection;

    private LookupRequest(InternalRow key, @Nullable int[] projection) {
        this.key = key;
        this.projection = projection == null ? null : projection.clone();
    }

    public static LookupRequest fullRow(InternalRow key) {
        return new LookupRequest(key, null);
    }

    public static LookupRequest projected(InternalRow key, int[] projection) {
        return new LookupRequest(key, projection);
    }

    public InternalRow key() {
        return key;
    }

    public Optional<int[]> projection() {
        return projection == null ? Optional.empty() : Optional.of(projection.clone());
    }
}
