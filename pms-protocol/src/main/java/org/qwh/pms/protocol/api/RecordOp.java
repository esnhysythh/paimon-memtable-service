package org.qwh.pms.protocol.api;

public enum RecordOp {
    PUT(0),
    DELETE(1);

    private final int code;

    RecordOp(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static RecordOp fromCode(int code) {
        for (RecordOp op : values()) {
            if (op.code == code) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown record op code: " + code);
    }
}
