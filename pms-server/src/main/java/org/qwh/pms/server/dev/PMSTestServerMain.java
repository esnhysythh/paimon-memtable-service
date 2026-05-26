package org.qwh.pms.server.dev;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.qwh.pms.server.ConfigManager;
import org.qwh.pms.server.PmsServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public final class PMSTestServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(PMSTestServerMain.class);
    private static final String DEFAULT_CONFIG_RESOURCE = "pms-test-server.properties";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private PMSTestServerMain() {}

    public static void main(String[] args) throws Exception {
        Properties raw = args.length == 0 ? loadDefaultProperties() : loadProperties(Path.of(args[0]));
        Properties expanded = expandProperties(raw);
        PmsServerConfig config = new ConfigManager().from(expanded);
        PMSTestServer server = PMSTestServer.create(config, defaultSchema()).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception e) {
                LOG.warn("Failed to close PMS test server cleanly", e);
            }
        }));

        LOG.info("PMS test server started at {}", server.baseUri());
        LOG.info("PMS test root: {}", expanded.getProperty("pms.test.root"));
        LOG.info("Try: " + curl(server.baseUri(), "/write", "{\"id\":1,\"marker\":\"hello\"}"));
        LOG.info("Try: " + curl(server.baseUri(), "/get", "{\"id\":1}"));
        LOG.info("Try: " + curl(server.baseUri(), "/flush", "{}")
            + " && " + curl(server.baseUri(), "/sink", "{}"));

        Thread.currentThread().join();
    }

    private static Schema defaultSchema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static String curl(java.net.URI baseUri, String path, String json) {
        return "curl -H 'Content-Type: application/json' -X POST "
            + baseUri.resolve(path)
            + " -d '"
            + json
            + "'";
    }

    public static Properties expandProperties(Properties raw) {
        Properties result = new Properties();
        result.putAll(raw);

        String timestamp = getString(result, "pms.test.timestamp", TIMESTAMP_FORMAT.format(LocalDateTime.now()));
        result.setProperty("pms.test.timestamp", timestamp);
        result.setProperty("timestamp", timestamp);
        result.setProperty("user.dir", System.getProperty("user.dir"));

        String root = expandValue(getString(result, "pms.test.root", "pms-server/target/pms-test-server/${timestamp}"), result);
        Path rootPath = Path.of(root).toAbsolutePath().normalize();
        result.setProperty("pms.test.root", rootPath.toString());

        for (String name : raw.stringPropertyNames()) {
            result.setProperty(name, expandValue(raw.getProperty(name), result));
        }

        return result;
    }

    private static Properties loadDefaultProperties() throws IOException {
        ClassLoader loader = PMSTestServerMain.class.getClassLoader();
        try (InputStream input = loader.getResourceAsStream(DEFAULT_CONFIG_RESOURCE)) {
            if (input == null) {
                throw new IllegalArgumentException("Default config resource not found: " + DEFAULT_CONFIG_RESOURCE);
            }
            Properties props = new Properties();
            props.load(input);
            return props;
        }
    }

    private static Properties loadProperties(Path path) throws IOException {
        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            props.load(input);
        }
        return props;
    }

    private static String expandValue(String value, Properties props) {
        String expanded = value;
        for (int i = 0; i < 8; i++) {
            String next = expanded;
            for (String name : props.stringPropertyNames()) {
                next = next.replace("${" + name + "}", props.getProperty(name));
            }
            if (next.equals(expanded)) {
                return next;
            }
            expanded = next;
        }
        return expanded;
    }

    private static String getString(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
