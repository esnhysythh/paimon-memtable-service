package org.qwh.pms.server;

public final class PmsServiceUnavailableException extends RuntimeException {
    public PmsServiceUnavailableException(String message) {
        super(message);
    }
}
