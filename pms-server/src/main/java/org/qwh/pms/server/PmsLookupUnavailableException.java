package org.qwh.pms.server;

public final class PmsLookupUnavailableException extends PmsServiceUnavailableException {
    public PmsLookupUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public PmsLookupUnavailableException(String message) {
        super(message);
    }
}
