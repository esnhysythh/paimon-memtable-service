package org.qwh.pms.core.config;

import java.util.Properties;

public record PMSConfig(
    MemTableConfig memtable,
    WalConfig wal,
    StorageConfig storage,
    SinkConfig sink,
    FlowControlConfig flowcontrol,
    PaimonConfig paimon
) {
    public PMSConfig {
        if (memtable == null) memtable = new MemTableConfig(0, 0);
        if (wal == null) throw new IllegalArgumentException("WAL config must not be null");
        if (storage == null) storage = new StorageConfig(null, 0, 0, 0, 0);
        if (sink == null) sink = new SinkConfig(0, 0);
        if (flowcontrol == null) flowcontrol = new FlowControlConfig(0, 0);
        if (paimon == null) throw new IllegalArgumentException("Paimon config must not be null");
    }

    public static PMSConfig from(Properties props) {
        return new PMSConfig(
            new MemTableConfig(
                getInt(props, "pms.memtable.max_entries", MemTableConfig.DEFAULT_MAX_ENTRIES),
                getInt(props, "pms.memtable.max_size_mb", MemTableConfig.DEFAULT_MAX_SIZE_MB)
            ),
            new WalConfig(
                getString(props, "pms.wal.dir", null),
                getInt(props, "pms.wal.file_size_mb", WalConfig.DEFAULT_FILE_SIZE_MB),
                getBoolean(props, "pms.wal.use_mmap", WalConfig.DEFAULT_USE_MMAP)
            ),
            new StorageConfig(
                getRequiredString(props, "pms.storage.dir"),
                getLong(props, "pms.storage.sinked_max_size_mb", StorageConfig.DEFAULT_SINKED_MAX_SIZE_MB),
                getInt(props, "pms.storage.sinked_max_count", StorageConfig.DEFAULT_SINKED_MAX_COUNT),
                getLong(props, "pms.storage.local_sst_max_rows", StorageConfig.DEFAULT_LOCAL_SST_MAX_ROWS),
                getInt(props, "pms.storage.compact_threshold_mb", StorageConfig.DEFAULT_COMPACT_THRESHOLD_MB),
                getInt(props, "pms.storage.compact_min_files", StorageConfig.DEFAULT_COMPACT_MIN_FILES)
            ),
            new SinkConfig(
                getInt(props, "pms.sink.interval_ms", SinkConfig.DEFAULT_INTERVAL_MS),
                getInt(props, "pms.sink.max_pending_ssts", SinkConfig.DEFAULT_MAX_PENDING_SSTS)
            ),
            new FlowControlConfig(
                getInt(props, "pms.flowcontrol.overloaded_immutable_count", FlowControlConfig.DEFAULT_OVERLOADED_IMMUTABLE_COUNT),
                getInt(props, "pms.flowcontrol.overloaded_pending_sst_count", FlowControlConfig.DEFAULT_OVERLOADED_PENDING_SST_COUNT)
            ),
            new PaimonConfig(
                getString(props, "pms.paimon.table_path", null),
                getString(props, "pms.paimon.warehouse", null)
            )
        );
    }

    private static int getInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for config key '" + key + "': " + value, e);
        }
    }

    private static long getLong(Properties props, String key, long defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for config key '" + key + "': " + value, e);
        }
    }

    private static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }

    private static String getString(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return (value == null || value.isBlank()) ? defaultValue : value.trim();
    }

    private static String getRequiredString(Properties props, String key) {
        String value = getString(props, key, null);
        if (value == null) {
            throw new IllegalArgumentException("Missing required config key: " + key);
        }
        return value;
    }
}
