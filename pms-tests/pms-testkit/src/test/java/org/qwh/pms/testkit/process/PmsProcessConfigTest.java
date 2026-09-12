package org.qwh.pms.testkit.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;

import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PmsProcessConfigTest {

    @Test
    void generatedConfigurationKeepsLocalStateSeparated(@TempDir Path tempDir) {
        Path phaseRoot = tempDir.resolve("phases/writer");
        PmsProcessConfig config = new PmsProcessConfig(
            "writer",
            "127.0.0.1",
            19090,
            URI.create("hdfs://namenode/tmp/pms-it/run/warehouse"),
            new PaimonTestTableSpec("pms_it_run", "smoke"),
            phaseRoot,
            tempDir.resolve("conf/server.properties"),
            tempDir.resolve("logs/server.out"),
            tempDir.resolve("run/server.pid")
        );

        Properties properties = config.properties("20260811-abcdef12");

        assertEquals("hdfs://namenode/tmp/pms-it/run/warehouse", properties.getProperty("pms.paimon.warehouse"));
        assertEquals("pms_it_run", properties.getProperty("pms.paimon.database"));
        assertEquals("smoke", properties.getProperty("pms.paimon.table"));
        assertEquals("false", properties.getProperty("pms.wal.use_mmap"));
        assertNotEquals(properties.getProperty("pms.wal.dir"), properties.getProperty("pms.storage.dir"));
        assertNotEquals(properties.getProperty("pms.wal.dir"), properties.getProperty("pms.lookup.cache.dir"));
    }
}
