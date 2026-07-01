package org.qwh.pms.protocol.api;

public enum PmsStatus {
    OK(0),
    BAD_REQUEST(1),
    OVERLOADED(2),
    SCHEMA_MISMATCH(3),
    SHUTTING_DOWN(4),
    INTERNAL_ERROR(5),
    LOOKUP_UNAVAILABLE(6);

    private final int code;

    PmsStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static PmsStatus fromCode(int code) {
        for (PmsStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown PMS status code: " + code);
    }
}
