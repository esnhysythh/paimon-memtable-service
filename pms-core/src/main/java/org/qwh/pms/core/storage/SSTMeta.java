package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;

import java.nio.file.Path;

public record SSTMeta(
    long runId,
    long minFlushId,
    long maxFlushId,
    Path path,
    long fileSize,
    long entryCount,
    Key minKey,
    Key maxKey,
    long minSequenceId,
    long maxSequenceId,
    long createdAtMillis,
    SSTState state,
    long refCount
) {
    public SSTMeta {
        if (runId <= 0) {
            throw new IllegalArgumentException("runId must be positive");
        }
        if (minFlushId <= 0 || maxFlushId <= 0 || minFlushId > maxFlushId) {
            throw new IllegalArgumentException("invalid flush bounds");
        }
        if (path == null) {
            throw new NullPointerException("path must not be null");
        }
        if (fileSize < 0) {
            throw new IllegalArgumentException("fileSize must be non-negative");
        }
        if (entryCount < 0) {
            throw new IllegalArgumentException("entryCount must be non-negative");
        }
        if (entryCount > 0) {
            if (minKey == null || maxKey == null) {
                throw new NullPointerException("minKey and maxKey must not be null when entryCount > 0");
            }
            if (minSequenceId <= 0 || maxSequenceId <= 0 || minSequenceId > maxSequenceId) {
                throw new IllegalArgumentException("invalid sequence bounds");
            }
        }
        if (state == null) {
            state = SSTState.NEW;
        }
    }

    public SSTMeta withPathAndState(Path path, SSTState state) {
        return new SSTMeta(
            runId,
            minFlushId,
            maxFlushId,
            path,
            fileSize,
            entryCount,
            minKey,
            maxKey,
            minSequenceId,
            maxSequenceId,
            createdAtMillis,
            state,
            refCount
        );
    }
}
