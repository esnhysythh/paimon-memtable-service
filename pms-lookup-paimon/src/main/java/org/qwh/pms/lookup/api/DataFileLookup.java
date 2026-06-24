package org.qwh.pms.lookup.api;

import org.apache.paimon.io.DataFileMeta;

import java.io.IOException;

/** Lookup path for one candidate Paimon data file. */
public interface DataFileLookup {

    LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request)
            throws IOException;

    void invalidate(FileLookupContext context, DataFileMeta file);

    /** Removes all local lookup state associated with a partition-bucket snapshot replacement. */
    default void invalidateBucket(FileLookupContext context) {
        // Direct lookup implementations may only keep per-file state.
    }
}
