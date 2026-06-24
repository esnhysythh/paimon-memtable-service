package org.qwh.pms.server;

public class PmsServiceUnavailableException extends RuntimeException {
    public PmsServiceUnavailableException(String message) {
        super(message);
    }

    public PmsServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
