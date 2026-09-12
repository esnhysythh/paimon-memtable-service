package org.qwh.pms.testkit.environment;

import java.util.Locale;

public enum CleanupPolicy {
    ON_SUCCESS("on-success"),
    ALWAYS("always"),
    NEVER("never");

    private final String propertyValue;

    CleanupPolicy(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public String propertyValue() {
        return propertyValue;
    }

    public boolean shouldCleanup(boolean successful) {
        return this == ALWAYS || (this == ON_SUCCESS && successful);
    }

    public static CleanupPolicy parse(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (CleanupPolicy policy : values()) {
            if (policy.propertyValue.equals(normalized)) {
                return policy;
            }
        }
        throw new IllegalArgumentException(
            "pms.it.cleanup must be one of on-success, always, never: " + value
        );
    }
}
