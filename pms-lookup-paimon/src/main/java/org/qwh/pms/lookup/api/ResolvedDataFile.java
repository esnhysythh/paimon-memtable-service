package org.qwh.pms.lookup.api;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

/** Resolved physical file information for a Paimon data file. */
public record ResolvedDataFile(FileIO fileIO, Path path, long fileSize) {}
