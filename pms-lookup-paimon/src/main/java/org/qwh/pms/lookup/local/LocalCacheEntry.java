package org.qwh.pms.lookup.local;

import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;

import java.io.Closeable;
import java.io.IOException;

/** Ready-to-query local cache entry for one Paimon data file. */
public interface LocalCacheEntry extends Closeable {

    LocalCacheMode mode();

    LookupResult lookup(LookupRequest request) throws IOException;

    /**
     * Returns the local disk space owned by this entry, or zero when the implementation cannot
     * determine it. Cache modes with on-disk state should override this for budget enforcement.
     */
    default long sizeBytes() {
        return 0L;
    }

    @Override
    void close() throws IOException;
}
