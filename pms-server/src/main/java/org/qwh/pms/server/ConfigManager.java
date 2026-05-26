package org.qwh.pms.server;

import org.qwh.pms.core.config.PMSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
            schedulerConfig(normalized, coreConfig),
            coreConfig
        );
        LOG.info(
            "PMS server config resolved: bind={}:{}, table={}.{}, walDir={}, storageDir={}, warehouse={}, schedulerEnabled={}",
            config.host(),
            config.port(),
            config.database(),
            config.table(),
            config.coreConfig().wal().dir(),
            config.coreConfig().storage().dir(),
            config.coreConfig().paimon().warehouse(),
            config.scheduler().enabled()
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

    private static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
