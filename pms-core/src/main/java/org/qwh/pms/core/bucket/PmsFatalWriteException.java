package org.qwh.pms.core.bucket;

/**
 * Signals that a write reached durable WAL but could not be fully applied in memory.
 *
 * <p>The write outcome is unknown to the caller and the current process must be restarted before
 * serving more requests.
 */
public final class PmsFatalWriteException extends IllegalStateException {

    public PmsFatalWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
