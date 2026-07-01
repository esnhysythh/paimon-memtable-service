package org.qwh.pms.client;

public class PmsClientException extends RuntimeException {

    public PmsClientException(String message) {
        super(message);
    }

    public PmsClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
