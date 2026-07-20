package org.qwh.pms.server;

import org.qwh.pms.core.config.PMSConfig;

public record PmsServerConfig(
    String host,
    int port,
    String database,
    String table,
    String commitUser,
    PmsProtocolConfig protocol,
    PmsSchedulerConfig scheduler,
    PmsLookupConfig lookup,
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
        if (protocol == null) {
            protocol = PmsProtocolConfig.defaults();
        }
        if (scheduler == null) {
            scheduler = PmsSchedulerConfig.defaults();
        }
        if (scheduler.newSstMaxCount() >= coreConfig.flowcontrol().overloadedPendingSstCount()) {
            throw new IllegalArgumentException(
                "pms.storage.new_sst.max_count must be lower than "
                    + "pms.flowcontrol.overloaded_pending_sst_count"
            );
        }
        if (lookup == null) {
            lookup = new PmsLookupConfig(
                PmsLookupConfig.DEFAULT_CACHE_ENABLED,
                PmsLookupConfig.defaultCacheDir(database, table),
                PmsLookupConfig.DEFAULT_MAX_CACHE_BYTES,
                PmsLookupConfig.DEFAULT_BUILD_THRESHOLD,
                PmsLookupConfig.DEFAULT_BUILD_THREADS,
                PmsLookupConfig.DEFAULT_BUILD_TIMEOUT,
                PmsLookupConfig.DEFAULT_RETRY_BACKOFF,
                PmsLookupConfig.DEFAULT_DIRECT_METADATA_CACHE_ENTRIES
            );
        }
    }
}
