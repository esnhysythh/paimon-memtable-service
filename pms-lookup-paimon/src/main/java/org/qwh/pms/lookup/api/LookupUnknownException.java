package org.qwh.pms.lookup.api;

/** Runtime exception for lookup failures that must be treated as UNKNOWN, not MISS. */
public final class LookupUnknownException extends RuntimeException {

    public LookupUnknownException(String message, Throwable cause) {
        super(message, cause);
    }
}
