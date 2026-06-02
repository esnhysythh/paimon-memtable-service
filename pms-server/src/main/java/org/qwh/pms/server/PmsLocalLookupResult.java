package org.qwh.pms.server;

import java.util.Map;

public record PmsLocalLookupResult(Type type, Map<String, Object> row) {
    public enum Type {
        HIT,
        DELETED,
        MISS
    }

    public static PmsLocalLookupResult hit(Map<String, Object> row) {
        return new PmsLocalLookupResult(Type.HIT, row);
    }

    public static PmsLocalLookupResult deleted() {
        return new PmsLocalLookupResult(Type.DELETED, null);
    }

    public static PmsLocalLookupResult miss() {
        return new PmsLocalLookupResult(Type.MISS, null);
    }

    public boolean found() {
        return type == Type.HIT;
    }
}
