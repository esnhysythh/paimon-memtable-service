package org.qwh.pms.core.config;

public record PaimonConfig(
    String tablePath,
    String warehouse
) {
    public PaimonConfig {
        if (tablePath == null || tablePath.isBlank()) {
            throw new IllegalArgumentException("Paimon table path must not be empty");
        }
    }
}
