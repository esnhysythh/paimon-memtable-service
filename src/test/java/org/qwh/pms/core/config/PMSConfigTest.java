package org.qwh.pms.core.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PMSConfigTest {

    @Test
    void fromWithDefaultsOnly() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/tmp/wal");
        props.setProperty("pms.paimon.table_path", "/tmp/table");

        PMSConfig config = PMSConfig.from(props);

        // MemTable defaults
        assertEquals(MemTableConfig.DEFAULT_MAX_ENTRIES, config.memtable().maxEntries());
        assertEquals(MemTableConfig.DEFAULT_MAX_SIZE_MB, config.memtable().maxSizeMb());

        // WAL required field
        assertEquals("/tmp/wal", config.wal().dir());
        assertEquals(WalConfig.DEFAULT_FILE_SIZE_MB, config.wal().fileSizeMb());
        assertFalse(config.wal().useMmap());

        // Storage defaults
        assertEquals(StorageConfig.DEFAULT_SINKED_MAX_SIZE_MB, config.storage().sinkedMaxSizeMb());
        assertEquals(StorageConfig.DEFAULT_SINKED_MAX_COUNT, config.storage().sinkedMaxCount());

        // Sink defaults
        assertEquals(SinkConfig.DEFAULT_INTERVAL_MS, config.sink().intervalMs());
        assertEquals(SinkConfig.DEFAULT_MAX_PENDING_SSTS, config.sink().maxPendingSsts());

        // Flow control defaults
        assertEquals(FlowControlConfig.DEFAULT_OVERLOADED_IMMUTABLE_COUNT, config.flowcontrol().overloadedImmutableCount());

        // Paimon required field
        assertEquals("/tmp/table", config.paimon().tablePath());
    }

    @Test
    void fromWithCustomValues() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/data/wal");
        props.setProperty("pms.paimon.table_path", "/data/table");
        props.setProperty("pms.memtable.max_entries", "500000");
        props.setProperty("pms.memtable.max_size_mb", "128");
        props.setProperty("pms.wal.file_size_mb", "512");
        props.setProperty("pms.wal.use_mmap", "true");
        props.setProperty("pms.sink.interval_ms", "60000");
        props.setProperty("pms.flowcontrol.overloaded_immutable_count", "8");

        PMSConfig config = PMSConfig.from(props);

        assertEquals(500000, config.memtable().maxEntries());
        assertEquals(128, config.memtable().maxSizeMb());
        assertEquals(512, config.wal().fileSizeMb());
        assertTrue(config.wal().useMmap());
        assertEquals(60000, config.sink().intervalMs());
        assertEquals(8, config.flowcontrol().overloadedImmutableCount());
    }

    @Test
    void fromMissingWalDirThrows() {
        Properties props = new Properties();
        props.setProperty("pms.paimon.table_path", "/tmp/table");

        assertThrows(IllegalArgumentException.class, () -> PMSConfig.from(props));
    }

    @Test
    void fromMissingPaimonTablePathThrows() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/tmp/wal");

        assertThrows(IllegalArgumentException.class, () -> PMSConfig.from(props));
    }

    @Test
    void fromInvalidIntThrowsWithKeyName() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/tmp/wal");
        props.setProperty("pms.paimon.table_path", "/tmp/table");
        props.setProperty("pms.memtable.max_entries", "not_a_number");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> PMSConfig.from(props));
        assertTrue(ex.getMessage().contains("pms.memtable.max_entries"),
            "Error message should contain the config key name");
    }

    @Test
    void fromInvalidLongThrowsWithKeyName() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/tmp/wal");
        props.setProperty("pms.paimon.table_path", "/tmp/table");
        props.setProperty("pms.storage.sinked_max_size_mb", "not_a_number");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> PMSConfig.from(props));
        assertTrue(ex.getMessage().contains("pms.storage.sinked_max_size_mb"),
            "Error message should contain the config key name");
    }

    @Test
    void fromBlankValueUsesDefault() {
        Properties props = new Properties();
        props.setProperty("pms.wal.dir", "/tmp/wal");
        props.setProperty("pms.paimon.table_path", "/tmp/table");
        props.setProperty("pms.memtable.max_entries", "   ");

        PMSConfig config = PMSConfig.from(props);
        assertEquals(MemTableConfig.DEFAULT_MAX_ENTRIES, config.memtable().maxEntries());
    }

    @Test
    void maxSizeBytesConversion() {
        MemTableConfig config = new MemTableConfig(0, 256);
        assertEquals(256L * 1024 * 1024, config.maxSizeBytes());
    }

    @Test
    void fileSizeBytesConversion() {
        WalConfig config = new WalConfig("/tmp", 256, false);
        assertEquals(256L * 1024 * 1024, config.fileSizeBytes());
    }
}
