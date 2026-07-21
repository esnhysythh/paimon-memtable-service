package org.qwh.pms.core.bucket;

/** The write was rejected before WAL append because the local maintenance backlog is overloaded. */
public final class PmsWriteOverloadedException extends RuntimeException {

    public PmsWriteOverloadedException(String message) {
        super(message);
    }
}
