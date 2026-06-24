package org.qwh.pms.lookup.api;

import org.apache.paimon.io.DataFileMeta;

import java.io.IOException;

/** Resolves a Paimon data file to a readable physical file in one partition-bucket. */
public interface DataFileResolver {

    ResolvedDataFile resolve(FileLookupContext context, DataFileMeta file) throws IOException;
}
