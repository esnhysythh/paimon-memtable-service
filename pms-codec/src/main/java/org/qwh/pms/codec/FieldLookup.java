package org.qwh.pms.codec;

public record FieldLookup(Kind kind, int notNullIndex) {

    public enum Kind {
        NOT_NULL,
        NULL,
        MISSING
    }

    public static FieldLookup notNull(int index) {
        return new FieldLookup(Kind.NOT_NULL, index);
    }

    public static FieldLookup nullValue() {
        return new FieldLookup(Kind.NULL, -1);
    }

    public static FieldLookup missing() {
        return new FieldLookup(Kind.MISSING, -1);
    }
}
