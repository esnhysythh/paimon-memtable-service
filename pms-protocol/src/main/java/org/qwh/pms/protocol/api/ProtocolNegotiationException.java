package org.qwh.pms.protocol.api;

public final class ProtocolNegotiationException extends RuntimeException {

    public ProtocolNegotiationException(String message) {
        super(message);
    }

    public ProtocolNegotiationException(String message, Throwable cause) {
        super(message, cause);
    }
}
