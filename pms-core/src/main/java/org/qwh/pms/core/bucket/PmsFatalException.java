package org.qwh.pms.core.bucket;

/**
 * The current bucket cannot serve further operations and must be restarted.
 *
 * <p>Write requests that receive this exception have an unknown outcome, including requests
 * rejected from the write queue. The exception does not describe how much was persisted.
 */
public final class PmsFatalException extends IllegalStateException {

    public PmsFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
