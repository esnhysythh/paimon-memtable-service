package org.qwh.pms.protocol.api;

public enum LookupResultType {
    HIT(0),
    MISS(1),
    DELETED(2);

    private final int code;

    LookupResultType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static LookupResultType fromCode(int code) {
        for (LookupResultType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown lookup result type code: " + code);
    }
}
