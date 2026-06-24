package org.qwh.pms.lookup.api;

/** Shared validation for lookup requests. */
public final class LookupRequestValidator {

    private LookupRequestValidator() {}

    public static void ensureKeyFieldCount(
            LookupRequest request, int expectedFieldCount, String context) {
        ensureKeyPresent(request);
        if (request.key().getFieldCount() != expectedFieldCount) {
            throw new IllegalArgumentException(
                    "Lookup key field count does not match "
                            + context
                            + ": request="
                            + request.key().getFieldCount()
                            + ", expected="
                            + expectedFieldCount);
        }
    }

    public static void ensureNonNullKey(LookupRequest request) {
        ensureKeyPresent(request);
        for (int i = 0; i < request.key().getFieldCount(); i++) {
            if (request.key().isNullAt(i)) {
                throw new IllegalArgumentException("Lookup key field must not be null: index=" + i);
            }
        }
    }

    private static void ensureKeyPresent(LookupRequest request) {
        if (request == null || request.key() == null) {
            throw new IllegalArgumentException("Lookup key must not be null.");
        }
    }
}
