package org.qwh.pms.server;

import org.qwh.pms.core.config.PMSConfig;

public record PmsServerConfig(
    String host,
    int port,
    String database,
    String table,
    String commitUser,
    PmsSchedulerConfig scheduler,
    PMSConfig coreConfig
) {
    public PmsServerConfig {
        if (host == null || host.isBlank()) {
            host = "127.0.0.1";
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid server port: " + port);
        }
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException("Paimon database must not be empty");
        }
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("Paimon table must not be empty");
        }
        if (commitUser == null || commitUser.isBlank()) {
            commitUser = "pms-server";
        }
        if (coreConfig == null) {
            throw new IllegalArgumentException("coreConfig must not be null");
        }
        if (scheduler == null) {
            scheduler = PmsSchedulerConfig.disabled(coreConfig.sink().intervalMs());
        }
    }
}
