package org.qwh.pms.testkit.process;

import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;
import org.qwh.pms.testkit.run.PmsTestRun;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

public record PmsProcessConfig(
        String phase,
        String host,
        int port,
        URI warehouse,
        PaimonTestTableSpec table,
        Path phaseRoot,
        Path propertiesPath,
        Path logPath,
        Path pidPath) {

    public PmsProcessConfig {
        if (phase == null || !phase.matches("[a-z][a-z0-9-]{0,47}")) {
            throw new IllegalArgumentException("Invalid PMS process phase: " + phase);
        }
        if (!"127.0.0.1".equals(host)) {
            throw new IllegalArgumentException("Integration PMS must bind to 127.0.0.1: " + host);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        Objects.requireNonNull(warehouse, "warehouse must not be null");
        Objects.requireNonNull(table, "table must not be null");
        phaseRoot = absolute(phaseRoot, "phaseRoot");
        propertiesPath = absolute(propertiesPath, "propertiesPath");
        logPath = absolute(logPath, "logPath");
        pidPath = absolute(pidPath, "pidPath");
    }

    public static PmsProcessConfig create(
            PmsTestRun run,
            String phase,
            PaimonTestTableSpec table) throws IOException {
        Path phaseRoot = run.phaseRoot(phase);
        return new PmsProcessConfig(
            phase,
            "127.0.0.1",
            availablePort(),
            run.warehouseUri(),
            table,
            phaseRoot,
            run.localRunRoot().resolve("conf").resolve("pms-server-" + phase + ".properties"),
            run.logPath(phase),
            run.localRunRoot().resolve("run").resolve("pms-server-" + phase + ".pid")
        );
    }

    public URI baseUri() {
        return URI.create("http://" + host + ":" + port + "/");
    }

    public Properties properties(String runId) {
        Properties properties = new Properties();
        properties.setProperty("pms.server.host", host);
        properties.setProperty("pms.server.port", Integer.toString(port));
        properties.setProperty("pms.server.commit_user", "pms-it-" + runId);
        properties.setProperty("pms.server.scheduler.flush_reconcile_interval_ms", "1000");
        properties.setProperty("pms.server.scheduler.maintenance_reconcile_interval_ms", "30000");
        // Explicit admin fences drive these correctness tests. Keep background visibility
        // outside their deadline; pre-kill state assertions still prove WAL-only coverage.
        properties.setProperty("pms.paimon.visibility.max_delay_ms", "3600000");

        properties.setProperty("pms.protocol.strict_http2", "true");
        properties.setProperty("pms.protocol.max_key_bytes", "65536");
        properties.setProperty("pms.protocol.max_row_bytes", "16777216");
        properties.setProperty("pms.protocol.max_batch_entries", "1024");
        properties.setProperty("pms.protocol.max_concurrent_streams", "128");
        properties.setProperty("pms.protocol.max_request_body_bytes", "33554432");
        properties.setProperty("pms.protocol.max_response_body_bytes", "33554432");

        properties.setProperty("pms.wal.dir", phaseRoot.resolve("wal").toString());
        properties.setProperty("pms.wal.file_size_mb", "64");
        properties.setProperty("pms.wal.use_mmap", "false");
        properties.setProperty("pms.storage.dir", phaseRoot.resolve("storage").toString());

        properties.setProperty("pms.lookup.cache.enabled", "true");
        properties.setProperty("pms.lookup.cache.dir", phaseRoot.resolve("lookup-cache").toString());
        properties.setProperty("pms.lookup.cache.max_bytes", "128m");
        properties.setProperty("pms.lookup.cache.build_threshold", "3");
        properties.setProperty("pms.lookup.cache.build_threads", "2");
        properties.setProperty("pms.lookup.cache.build_timeout_ms", "30000");
        properties.setProperty("pms.lookup.cache.retry_backoff_ms", "60000");
        properties.setProperty("pms.lookup.direct.metadata_cache_entries", "256");

        properties.setProperty("pms.paimon.warehouse", warehouse.toString());
        properties.setProperty("pms.paimon.database", table.database());
        properties.setProperty("pms.paimon.table", table.table());
        properties.setProperty("pms.paimon.cache_enabled", "true");
        properties.setProperty("pms.paimon.manifest_cache_small_file_memory", "32mb");
        properties.setProperty("pms.paimon.manifest_cache_small_file_threshold", "1mb");

        properties.setProperty("pms.memtable.max_entries", "1000000");
        properties.setProperty("pms.memtable.max_size_mb", "64");
        properties.setProperty("pms.storage.new_sst.max_count", "10");
        properties.setProperty("pms.storage.sinked_sst.max_count", "10");
        properties.setProperty("pms.operation.sink.batch_max_bytes_mb", "256");
        properties.setProperty("pms.operation.compact.max_input_size_mb", "256");
        properties.setProperty("pms.flowcontrol.overloaded_immutable_count", "4");
        properties.setProperty("pms.flowcontrol.overloaded_pending_sst_count", "20");
        return properties;
    }

    public void writeProperties(String runId) throws IOException {
        Files.createDirectories(propertiesPath.getParent());
        Files.createDirectories(logPath.getParent());
        Files.createDirectories(pidPath.getParent());
        Files.createDirectories(phaseRoot.resolve("wal"));
        Files.createDirectories(phaseRoot.resolve("storage"));
        Files.createDirectories(phaseRoot.resolve("lookup-cache"));
        Files.createDirectories(phaseRoot.resolve("tmp"));
        try (OutputStream output = Files.newOutputStream(propertiesPath)) {
            properties(runId).store(output, "Generated by pms-testkit; do not reuse outside this run");
        }
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }

    private static Path absolute(Path value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (!value.isAbsolute()) {
            throw new IllegalArgumentException(name + " must be absolute: " + value);
        }
        return value.normalize();
    }
}
