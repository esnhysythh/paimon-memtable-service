package org.qwh.pms.server;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.server.dev.PMSTestServer;
import org.qwh.pms.sink.paimon.PaimonSinkManager;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsServerEndToEndTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void httpApiDrivesWriteGetFlushSinkAndRecovery() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            Map<String, Object> initialState = server.getJson("/state");
            @SuppressWarnings("unchecked")
            Map<String, Object> runtime = (Map<String, Object>) initialState.get("runtime");
            assertEquals("RUNNING", runtime.get("status"));
            assertEquals(true, runtime.get("acceptingWrites"));

            assertJson(server.post("/write", "{\"id\":1,\"marker\":\"old-a\"}"), "\"status\":\"OK\"");
            assertJson(server.post("/get", "{\"id\":1}"), "\"found\":true");
            assertJson(server.post("/flush", "{}"), "\"status\":\"OK\"");
            assertJson(server.post("/sink", "{}"), "\"status\":\"OK\"");
            assertJson(server.get("/state"), "\"sinkedSSTCount\":1");

            assertEquals(Map.of(1, "old-a"), server.readIntStringRows());

            server.restart();
            assertEquals(
                Map.of("id", 1, "marker", "old-a"),
                server.get(Map.of("id", 1)).orElseThrow()
            );
            assertFalse(server.get(Map.of("id", 2)).isPresent());
        }
    }

    @Test
    void runtimeCloseFlushesAndSinksRemainingData() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "close-a"));

            PmsServerRuntime runtime = server.runtime();
            runtime.close();

            assertEquals(PmsRuntimeStatus.STOPPED, runtime.status());
            assertEquals(Map.of(1, "close-a"), server.readIntStringRows());
            assertThrows(
                PmsServiceUnavailableException.class,
                () -> runtime.write(Map.of("id", 2, "marker", "rejected"))
            );
        }
    }

    @Test
    void unflushedWalDataIsRecoveredAfterAbortRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "wal-a"));

            server.abortAndRestart();

            assertEquals(Map.of("id", 1, "marker", "wal-a"), server.get(Map.of("id", 1)).orElseThrow());
            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(1L, number(recovery, "recoveredDataRecords"));
            assertEquals(0L, number(recovery, "skippedFlushedRecords"));
            assertEquals(0L, number(recovery, "lastFlushedSequenceId"));
            assertEquals(1L, number(recovery, "curMemTableEstimatedEntryCount"));
        }
    }

    @Test
    void flushedUnsinkedSstIsRecoveredAfterAbortRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "sst-a"));
            server.flush();

            server.abortAndRestart();

            assertEquals(Map.of("id", 1, "marker", "sst-a"), server.get(Map.of("id", 1)).orElseThrow());
            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(0L, number(recovery, "recoveredDataRecords"));
            assertEquals(1L, number(recovery, "skippedFlushedRecords"));
            assertEquals(1L, number(recovery, "lastFlushedSequenceId"));
            assertEquals(1L, number(recovery, "newSSTCount"));

            server.sink();
            assertEquals(Map.of(1, "sst-a"), server.readIntStringRows());
        }
    }

    @Test
    void preparedSinkWithoutSuccessIsCommittedDuringRecovery() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema())) {
            PmsServerRuntime failingRuntime = new PmsServerRuntime(
                server.config(),
                (table, commitUser, storage) ->
                    new FailAfterPrepareSinkManager(new PaimonSinkManager(table, commitUser, storage))
            ).start();

            try {
                failingRuntime.write(Map.of("id", 1, "marker", "prepared-a"));
                failingRuntime.flush();

                RuntimeException error = assertThrows(RuntimeException.class, failingRuntime::sink);
                assertTrue(rootCauseMessage(error).contains("forced commit failure after prepare"));
            } finally {
                failingRuntime.abort();
            }

            PmsServerRuntime recoveredRuntime = new PmsServerRuntime(server.config()).start();
            try {
                assertEquals(
                    Map.of("id", 1, "marker", "prepared-a"),
                    recoveredRuntime.get(Map.of("id", 1)).orElseThrow()
                );
                Map<String, Object> recovery = recovery(recoveredRuntime.state());
                assertEquals(1L, number(recovery, "pendingPreparedSinkCount"));
                assertEquals(1L, number(recovery, "recoveredPreparedSinkCount"));
                assertEquals(1L, number(recovery, "recoveredSinkedSSTCount"));
                assertEquals(1L, number(recovery, "lastSinkedSnapshotId"));
                assertEquals(0L, number(recovery, "newSSTCount"));
                assertEquals(1L, number(recovery, "sinkedSSTCount"));
                assertEquals(Map.of(1, "prepared-a"), server.readIntStringRows());
            } finally {
                recoveredRuntime.close();
            }
        }
    }

    @Test
    void sinkSuccessWalStateIsRecoveredAfterAbortRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "success-a"));
            server.flush();
            server.sink();

            server.abortAndRestart();

            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(0L, number(recovery, "pendingPreparedSinkCount"));
            assertEquals(0L, number(recovery, "recoveredPreparedSinkCount"));
            assertEquals(1L, number(recovery, "recoveredSinkedSSTCount"));
            assertEquals(1L, number(recovery, "lastSinkedSnapshotId"));
            assertEquals(0L, number(recovery, "newSSTCount"));
            assertEquals(1L, number(recovery, "sinkedSSTCount"));
            assertEquals(Map.of("id", 1, "marker", "success-a"), server.get(Map.of("id", 1)).orElseThrow());
            assertEquals(Map.of(1, "success-a"), server.readIntStringRows());
        }
    }

    @Test
    void missingFlushedSstFailsStartupInsteadOfDroppingData() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "missing-a"));
            server.flush();
            server.abortRuntime();
        }

        Files.delete(flushedSstPath());

        PmsServerRuntime runtime = new PmsServerRuntime(new ConfigManager().from(baseProperties()));
        Exception error = assertThrows(Exception.class, runtime::start);
        assertTrue(causalMessages(error).contains("SST files are missing after flush boundary was persisted"));
        assertEquals(PmsRuntimeStatus.FAILED, runtime.status());
    }

    @Test
    void corruptFlushedSstFailsStartupWithDiagnosticError() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "corrupt-a"));
            server.flush();
            server.abortRuntime();
        }

        Files.write(flushedSstPath(), new byte[] {1, 2, 3, 4});

        PmsServerRuntime runtime = new PmsServerRuntime(new ConfigManager().from(baseProperties()));
        Exception error = assertThrows(Exception.class, runtime::start);
        assertTrue(causalMessages(error).contains("SST file is corrupt after flush boundary was persisted"));
        assertEquals(PmsRuntimeStatus.FAILED, runtime.status());
    }

    @Test
    void runtimeStartFailureIsRecordedAsFailed() throws Exception {
        PmsServerConfig config = new ConfigManager().from(baseProperties());
        PmsServerRuntime runtime = new PmsServerRuntime(config);

        assertThrows(Exception.class, runtime::start);

        assertEquals(PmsRuntimeStatus.FAILED, runtime.status());
        @SuppressWarnings("unchecked")
        Map<String, Object> runtimeState = (Map<String, Object>) runtime.state().get("runtime");
        assertEquals("FAILED", runtimeState.get("status"));
        assertTrue(runtimeState.get("failedAt") != null);
        assertTrue(runtimeState.get("lastFailureMessage") != null);
    }

    @Test
    void deleteByPrimaryKeySinksTombstoneToPaimon() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "old-a"));
            server.flush();
            server.sink();
            assertEquals(Map.of(1, "old-a"), server.readIntStringRows());

            server.delete(Map.of("id", 1));
            server.flush();
            server.sink();

            assertEquals(Map.of(), server.readIntStringRows());
        }
    }

    @Test
    void schedulerAutomaticallyFlushesAndSinks() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.server.scheduler.enabled", "true");
        props.setProperty("pms.server.scheduler.flush_interval_ms", "50");
        props.setProperty("pms.server.scheduler.sink_interval_ms", "50");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "auto-a"));

            waitUntil(() -> {
                try {
                    return Map.of(1, "auto-a").equals(server.readIntStringRows());
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> state = server.getJson("/state");
            @SuppressWarnings("unchecked")
            Map<String, Object> scheduler = (Map<String, Object>) state.get("scheduler");
            assertEquals(true, scheduler.get("enabled"));
            assertEquals(true, scheduler.get("running"));
            assertTrue((Integer) scheduler.get("flushIntervalMs") > 0);
            assertTrue((Integer) scheduler.get("sinkIntervalMs") > 0);
            assertEquals(false, scheduler.get("flushRunning"));
            assertEquals(false, scheduler.get("sinkRunning"));
            assertTrue(number(scheduler, "flushSuccessCount") > 0);
            assertTrue(number(scheduler, "sinkSuccessCount") > 0);
            assertEquals(0L, number(scheduler, "flushFailureCount"));
            assertEquals(0L, number(scheduler, "sinkFailureCount"));
            assertTrue(scheduler.containsKey("lastFlushStartedAt"));
            assertTrue(scheduler.containsKey("lastFlushCompletedAt"));
            assertTrue(scheduler.containsKey("lastSinkStartedAt"));
            assertTrue(scheduler.containsKey("lastSinkCompletedAt"));
            assertTrue(number(scheduler, "lastFlushDurationMs") >= 0);
            assertTrue(number(scheduler, "lastSinkDurationMs") >= 0);
        }
    }

    @Test
    void configManagerLoadsPropertiesFile() throws Exception {
        Properties props = baseProperties();
        java.nio.file.Path configFile = tempDir.resolve("pms-server.properties");
        try (var output = Files.newOutputStream(configFile)) {
            props.store(output, "test PMS server config");
        }

        PmsServerConfig config = new ConfigManager().load(configFile);

        assertEquals("127.0.0.1", config.host());
        assertEquals(0, config.port());
        assertEquals("pms_db", config.database());
        assertEquals("server_pk", config.table());
        assertEquals(tempDir.resolve("wal").toString(), config.coreConfig().wal().dir());
        assertEquals(tempDir.resolve("storage").toString(), config.coreConfig().storage().dir());
        assertFalse(config.scheduler().enabled());
        assertEquals(0, config.scheduler().flushIntervalMs());
        assertEquals(30000, config.scheduler().sinkIntervalMs());
    }

    @Test
    void configManagerRejectsSameWalAndStorageDirectory() {
        Properties props = baseProperties();
        props.setProperty("pms.storage.dir", props.getProperty("pms.wal.dir"));

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(props)
        );
        assertTrue(error.getMessage().contains("must not be the same directory"));
    }

    @Test
    void testServerMainExpandsTimestampedTargetDirectories() {
        Properties props = new Properties();
        props.setProperty("pms.test.timestamp", "20260526100000");
        props.setProperty("pms.test.root", "pms-server/target/pms-test-server/${timestamp}");
        props.setProperty("pms.server.port", "19090");
        props.setProperty("pms.paimon.database", "pms_db");
        props.setProperty("pms.paimon.table", "server_pk");
        props.setProperty("pms.paimon.warehouse", "file:${pms.test.root}/paimon");
        props.setProperty("pms.wal.dir", "${pms.test.root}/wal");
        props.setProperty("pms.storage.dir", "${pms.test.root}/storage");

        Properties expanded = org.qwh.pms.server.dev.PMSTestServerMain.expandProperties(props);

        assertTrue(expanded.getProperty("pms.test.root").endsWith("pms-server/target/pms-test-server/20260526100000"));
        assertTrue(expanded.getProperty("pms.wal.dir").endsWith("pms-server/target/pms-test-server/20260526100000/wal"));
        assertTrue(expanded.getProperty("pms.storage.dir").endsWith("pms-server/target/pms-test-server/20260526100000/storage"));
        assertTrue(expanded.getProperty("pms.paimon.warehouse").endsWith("pms-server/target/pms-test-server/20260526100000/paimon"));
    }

    private static Schema schema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static void assertJson(String json, String expected) {
        assertTrue(json.contains(expected), () -> "Expected " + expected + " in " + json);
    }

    private Properties baseProperties() {
        Properties props = new Properties();
        props.setProperty("pms.server.host", "127.0.0.1");
        props.setProperty("pms.server.port", "0");
        props.setProperty("pms.paimon.warehouse", tempDir.resolve("warehouse").toUri().toString());
        props.setProperty("pms.paimon.database", "pms_db");
        props.setProperty("pms.paimon.table", "server_pk");
        props.setProperty("pms.wal.dir", tempDir.resolve("wal").toString());
        props.setProperty("pms.storage.dir", tempDir.resolve("storage").toString());
        return props;
    }

    private Path flushedSstPath() {
        return tempDir.resolve("storage").resolve("sst-000001.new.sst");
    }

    private static void waitUntil(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true before timeout");
    }

    private static long number(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).longValue();
    }

    private static String rootCauseMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage();
    }

    private static String causalMessages(Throwable error) {
        StringBuilder messages = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            messages.append(current.getMessage()).append('\n');
            current = current.getCause();
        }
        return messages.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> recovery(Map<String, Object> state) {
        Map<String, Object> runtime = (Map<String, Object>) state.get("runtime");
        return (Map<String, Object>) runtime.get("recovery");
    }

    private static final class FailAfterPrepareSinkManager implements SinkManager {
        private final SinkManager delegate;

        private FailAfterPrepareSinkManager(SinkManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) {
            return delegate.prepare(batch);
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            throw new RuntimeException("forced commit failure after prepare");
        }
    }
}
