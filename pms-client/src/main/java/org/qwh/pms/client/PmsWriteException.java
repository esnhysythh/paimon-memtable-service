package org.qwh.pms.client;

import org.qwh.pms.protocol.api.PmsStatus;

public final class PmsWriteException extends PmsClientException {

    private final PmsStatus status;

    public PmsWriteException(PmsStatus status, String message) {
        super(message);
        this.status = status;
    }

    public PmsStatus status() {
        return status;
    }
}
