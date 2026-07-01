package org.qwh.pms.server;

import org.qwh.pms.core.config.PMSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

public final class ConfigManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);

    public PmsServerConfig load(Path path) throws IOException {
        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            props.load(input);
        }
        LOG.info("Loaded PMS server config file: {}", path.toAbsolutePath());
        return from(props);
    }

    public PmsServerConfig from(Properties props) {
        Properties normalized = new Properties();
        normalized.putAll(props);
        String database = getString(normalized, "pms.paimon.database", null);
        String table = getString(normalized, "pms.paimon.table", null);
        if (getString(normalized, "pms.paimon.table_path", null) == null
                && database != null
                && table != null) {
            normalized.setProperty("pms.paimon.table_path", database + "." + table);
        }

        validateLocalStateDirs(normalized);

        PMSConfig coreConfig = PMSConfig.from(normalized);
        PmsServerConfig config = new PmsServerConfig(
            getString(normalized, "pms.server.host", "127.0.0.1"),
            getInt(normalized, "pms.server.port", 9090),
            database,
            table,
            getString(normalized, "pms.server.commit_user", "pms-server"),
            protocolConfig(normalized),
            schedulerConfig(normalized, coreConfig),
            lookupConfig(normalized, coreConfig, database, table),
            coreConfig
        );
        LOG.info(
            "PMS server config resolved: bind={}:{}, table={}.{}, walDir={}, storageDir={}, warehouse={}, schedulerEnabled={}, lookupCacheEnabled={}, lookupCacheDir={}",
            config.host(),
            config.port(),
            config.database(),
            config.table(),
            config.coreConfig().wal().dir(),
            config.coreConfig().storage().dir(),
            config.coreConfig().paimon().warehouse(),
            config.scheduler().enabled(),
            config.lookup().cacheEnabled(),
            config.lookup().cacheDir()
        );
        return config;
    }

    private static PmsSchedulerConfig schedulerConfig(Properties props, PMSConfig coreConfig) {
        return new PmsSchedulerConfig(
            getBoolean(props, "pms.server.scheduler.enabled", PmsSchedulerConfig.DEFAULT_ENABLED),
            getInt(props, "pms.server.scheduler.flush_interval_ms", PmsSchedulerConfig.DEFAULT_FLUSH_INTERVAL_MS),
            getInt(props, "pms.server.scheduler.sink_interval_ms", coreConfig.sink().intervalMs())
        );
    }

    private static PmsProtocolConfig protocolConfig(Properties props) {
        return new PmsProtocolConfig(
            getBoolean(props, "pms.protocol.strict_http2", PmsProtocolConfig.DEFAULT_STRICT_HTTP2),
            getInt(props, "pms.protocol.max_key_bytes", PmsProtocolConfig.DEFAULT_MAX_KEY_BYTES),
            getInt(props, "pms.protocol.max_row_bytes", PmsProtocolConfig.DEFAULT_MAX_ROW_BYTES),
            getInt(props, "pms.protocol.max_batch_entries", PmsProtocolConfig.DEFAULT_MAX_BATCH_ENTRIES),
            getInt(props, "pms.protocol.max_concurrent_streams", PmsProtocolConfig.DEFAULT_MAX_CONCURRENT_STREAMS),
            getInt(props, "pms.protocol.max_request_body_bytes", PmsProtocolConfig.DEFAULT_MAX_REQUEST_BODY_BYTES),
            getInt(props, "pms.protocol.max_response_body_bytes", PmsProtocolConfig.DEFAULT_MAX_RESPONSE_BODY_BYTES)
        );
    }

    private static PmsLookupConfig lookupConfig(
            Properties props, PMSConfig coreConfig, String database, String table) {
        Path defaultCacheDir = PmsLookupConfig.defaultCacheDir(database, table);
        Path cacheDir = Path.of(getString(
            props,
            "pms.lookup.cache.dir",
            defaultCacheDir.toString()
        ));
        PmsLookupConfig config = new PmsLookupConfig(
            getBoolean(props, "pms.lookup.cache.enabled", PmsLookupConfig.DEFAULT_CACHE_ENABLED),
            cacheDir,
            getBytes(props, "pms.lookup.cache.max_bytes", PmsLookupConfig.DEFAULT_MAX_CACHE_BYTES),
            getInt(props, "pms.lookup.cache.build_threshold", PmsLookupConfig.DEFAULT_BUILD_THRESHOLD),
            getInt(props, "pms.lookup.cache.build_threads", PmsLookupConfig.DEFAULT_BUILD_THREADS),
            getDurationMillis(
                props,
                "pms.lookup.cache.build_timeout_ms",
                PmsLookupConfig.DEFAULT_BUILD_TIMEOUT
            ),
            getDurationMillis(
                props,
                "pms.lookup.cache.retry_backoff_ms",
                PmsLookupConfig.DEFAULT_RETRY_BACKOFF
            ),
            getInt(
                props,
                "pms.lookup.direct.metadata_cache_entries",
                PmsLookupConfig.DEFAULT_DIRECT_METADATA_CACHE_ENTRIES
            )
        );
        validateLookupCacheDir(config.cacheDir(), coreConfig);
        return config;
    }

    private static void validateLocalStateDirs(Properties props) {
        String walDir = getString(props, "pms.wal.dir", null);
        String storageDir = getString(props, "pms.storage.dir", null);
        if (walDir == null) {
            throw new IllegalArgumentException("Missing required config key: pms.wal.dir");
        }
        if (storageDir == null) {
            throw new IllegalArgumentException("Missing required config key: pms.storage.dir");
        }
        Path walPath = Path.of(walDir).toAbsolutePath().normalize();
        Path storagePath = Path.of(storageDir).toAbsolutePath().normalize();
        if (walPath.equals(storagePath)) {
            throw new IllegalArgumentException("pms.wal.dir and pms.storage.dir must not be the same directory");
        }
    }

    private static void validateLookupCacheDir(Path cacheDir, PMSConfig coreConfig) {
        Path normalizedCacheDir = cacheDir.toAbsolutePath().normalize();
        Path walDir = Path.of(coreConfig.wal().dir()).toAbsolutePath().normalize();
        Path storageDir = Path.of(coreConfig.storage().dir()).toAbsolutePath().normalize();
        if (normalizedCacheDir.equals(walDir) || normalizedCacheDir.startsWith(walDir)) {
            throw new IllegalArgumentException("pms.lookup.cache.dir must not overlap pms.wal.dir");
        }
        if (normalizedCacheDir.equals(storageDir) || normalizedCacheDir.startsWith(storageDir)) {
            throw new IllegalArgumentException("pms.lookup.cache.dir must not overlap pms.storage.dir");
        }
        localPath(coreConfig.paimon().warehouse()).ifPresent(warehouse -> {
            Path normalizedWarehouse = warehouse.toAbsolutePath().normalize();
            if (normalizedCacheDir.equals(normalizedWarehouse)
                    || normalizedCacheDir.startsWith(normalizedWarehouse)) {
                throw new IllegalArgumentException(
                    "pms.lookup.cache.dir must not be inside the local Paimon warehouse");
            }
        });
    }

    private static java.util.Optional<Path> localPath(String value) {
        if (value == null || value.isBlank()) {
            return java.util.Optional.empty();
        }
        try {
            URI uri = new URI(value);
            if (uri.getScheme() == null) {
                return java.util.Optional.of(Path.of(value));
            }
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                return java.util.Optional.of(Path.of(uri));
            }
            return java.util.Optional.empty();
        } catch (URISyntaxException | IllegalArgumentException e) {
            return java.util.Optional.of(Path.of(value));
        }
    }

    private static String getString(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static int getInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static long getBytes(Properties props, String key, long defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return parseBytes(value.trim());
    }

    private static Duration getDurationMillis(Properties props, String key, Duration defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Duration.ofMillis(Long.parseLong(value.trim()));
    }

    private static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    private static long parseBytes(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT).replace("_", "");
        long multiplier = 1L;
        if (normalized.endsWith("kb") || normalized.endsWith("k")) {
            multiplier = 1024L;
            normalized = normalized.replaceFirst("kb?$", "");
        } else if (normalized.endsWith("mb") || normalized.endsWith("m")) {
            multiplier = 1024L * 1024;
            normalized = normalized.replaceFirst("mb?$", "");
        } else if (normalized.endsWith("gb") || normalized.endsWith("g")) {
            multiplier = 1024L * 1024 * 1024;
            normalized = normalized.replaceFirst("gb?$", "");
        } else if (normalized.endsWith("b")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return Math.multiplyExact(Long.parseLong(normalized.trim()), multiplier);
    }
}
