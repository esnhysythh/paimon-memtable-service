package org.qwh.pms.server;

final class PmsOverloadedException extends PmsServiceUnavailableException {

    PmsOverloadedException(String message) {
        super(message);
    }

    PmsOverloadedException(String message, Throwable cause) {
        super(message, cause);
    }
}
