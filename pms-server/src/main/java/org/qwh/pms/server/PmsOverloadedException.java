package org.qwh.pms.server;

final class PmsOverloadedException extends PmsServiceUnavailableException {

    PmsOverloadedException(String message) {
        super(message);
    }
}
