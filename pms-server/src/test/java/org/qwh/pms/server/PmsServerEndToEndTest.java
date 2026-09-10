package org.qwh.pms.server;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.client.PmsClientConfig;
import org.qwh.pms.client.PmsRowLookupBatchResult;
import org.qwh.pms.client.PmsRowLookupResult;
import org.qwh.pms.client.PmsRawBatchWriter;
import org.qwh.pms.client.PmsRawClient;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.bucket.PMSBucketDirector;
import org.qwh.pms.core.bucket.PmsFatalWriteException;
import org.qwh.pms.core.bucket.SinkFlightSnapshot;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.sink.PreparedSinkCommit;
import org.qwh.pms.core.sink.SinkBatch;
import org.qwh.pms.core.sink.SinkCommitResult;
import org.qwh.pms.core.sink.SinkManager;
import org.qwh.pms.core.storage.FileLocalStorageManager;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.WriteResult;
import org.qwh.pms.protocol.codec.KeyBatchCodec;
import org.qwh.pms.protocol.codec.LookupBatchCodec;
import org.qwh.pms.protocol.codec.RecordBatchCodec;
import org.qwh.pms.protocol.codec.WriteResultCodec;
import org.qwh.pms.server.dev.PMSTestServer;
import org.qwh.pms.server.PmsLocalLookupResult.Type;
import org.qwh.pms.sink.paimon.PaimonSinkManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            PMSTestServer.HttpResult flushResponse = server.postResult("/flush", "{}");
            assertEquals(202, flushResponse.statusCode());
            Map<String, Object> flushAccepted = flushResponse.jsonObject();
            assertEquals("ACCEPTED", flushAccepted.get("status"));
            assertEquals("FLUSH", flushAccepted.get("operation"));
            long flushFence = number(flushAccepted, "fenceSequenceId");
            waitUntil(() -> boundaryReached(server, "lastFlushedSequenceId", flushFence));

            PMSTestServer.HttpResult sinkResponse = server.postResult("/sink", "{}");
            assertEquals(202, sinkResponse.statusCode());
            Map<String, Object> sinkAccepted = sinkResponse.jsonObject();
            assertEquals("ACCEPTED", sinkAccepted.get("status"));
            assertEquals("SINK", sinkAccepted.get("operation"));
            long sinkFence = number(sinkAccepted, "fenceSequenceId");
            waitUntil(() -> boundaryReached(server, "lastPersistedSequenceId", sinkFence));
            assertEquals(1L, number(server.getJson("/state"), "sinkedSSTCount"));

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
    void runtimeCloseReliesOnWalRecoveryWithoutFlushingOrSinking() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "close-a"));

            PmsServerRuntime runtime = server.runtime();
            runtime.close();

            assertEquals(PmsRuntimeStatus.STOPPED, runtime.status());
            assertEquals(Map.of(), server.readIntStringRows());
            assertThrows(
                PmsServiceUnavailableException.class,
                () -> runtime.write(Map.of("id", 2, "marker", "rejected"))
            );
            assertThrows(PmsServiceUnavailableException.class, runtime::flush);
            assertThrows(PmsServiceUnavailableException.class, runtime::sink);

            server.restart();
            assertEquals(
                Map.of("id", 1, "marker", "close-a"),
                server.get(Map.of("id", 1)).orElseThrow()
            );
            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(1L, number(recovery, "recoveredDataRecords"));
        }
    }

    @Test
    void unflushedWalDataIsRecoveredAfterRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "wal-a"));

            server.restart();

            assertEquals(Map.of("id", 1, "marker", "wal-a"), server.get(Map.of("id", 1)).orElseThrow());
            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(1L, number(recovery, "recoveredDataRecords"));
            assertEquals(0L, number(recovery, "skippedFlushedRecords"));
            assertEquals(0L, number(recovery, "lastFlushedSequenceId"));
            assertEquals(1L, number(recovery, "curMemTableEstimatedEntryCount"));
        }
    }

    @Test
    void flushedUnsinkedSstIsRecoveredAfterRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "sst-a"));
            flushAndWait(server);

            server.restart();

            assertEquals(Map.of("id", 1, "marker", "sst-a"), server.get(Map.of("id", 1)).orElseThrow());
            Map<String, Object> recovery = recovery(server.getJson("/state"));
            assertEquals(0L, number(recovery, "recoveredDataRecords"));
            assertEquals(1L, number(recovery, "skippedFlushedRecords"));
            assertEquals(1L, number(recovery, "lastFlushedSequenceId"));
            assertEquals(1L, number(recovery, "newSSTCount"));

            sinkAndWait(server);
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
                failingRuntime.sink();
                waitUntil(() -> sinkFlight(failingRuntime) == SinkFlightSnapshot.Status.PREPARED_RETRY);
            } finally {
                failingRuntime.close();
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
    void preparedSinkCommitRetriesInSameRuntimeWithoutPreparingAgain() throws Exception {
        AtomicReference<FailOnceAfterPrepareSinkManager> sinkManagerRef = new AtomicReference<>();
        Properties props = baseProperties();
        props.setProperty("pms.server.scheduler.maintenance_reconcile_interval_ms", "25");
        PmsServerConfig config = new ConfigManager().from(props);
        try (PMSTestServer server = PMSTestServer.create(config, schema())) {
            PmsServerRuntime runtime = new PmsServerRuntime(
                server.config(),
                (table, commitUser, storage) -> {
                    FailOnceAfterPrepareSinkManager manager = new FailOnceAfterPrepareSinkManager(
                        new PaimonSinkManager(table, commitUser, storage)
                    );
                    sinkManagerRef.set(manager);
                    return manager;
                }
            ).start();
            try {
                runtime.write(Map.of("id", 1, "marker", "online-retry-a"));
                long fence = runtime.sink();
                FailOnceAfterPrepareSinkManager manager = sinkManagerRef.get();
                assertNotNull(manager);
                waitUntil(() -> manager.commitCalls >= 2 && boundaryReached(
                    runtime,
                    "lastPersistedSequenceId",
                    fence
                ));

                assertEquals(1, manager.prepareCalls);
                assertEquals(2, manager.commitCalls);
                assertEquals(manager.firstCommit.batchId(), manager.secondCommit.batchId());
                assertEquals(manager.firstCommit.commitIdentifier(), manager.secondCommit.commitIdentifier());
                assertEquals(manager.firstCommit.sstIds(), manager.secondCommit.sstIds());
                assertArrayEquals(manager.firstCommit.payload(), manager.secondCommit.payload());
                assertEquals(
                    Map.of("id", 1, "marker", "online-retry-a"),
                    runtime.get(Map.of("id", 1)).orElseThrow()
                );
                assertEquals(Map.of(1, "online-retry-a"), server.readIntStringRows());
                assertEquals(
                    SinkFlightSnapshot.Status.IDLE,
                    ((SinkFlightSnapshot) runtime.state().get("sinkFlight")).status()
                );
            } finally {
                runtime.close();
            }
        }
    }

    @Test
    void sinkSuccessWalStateIsRecoveredAfterRestart() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "success-a"));
            flushAndWait(server);
            sinkAndWait(server);

            server.restart();

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
    void freshLocalStateCanContinueWritingToAnExistingPaimonTable() throws Exception {
        PmsServerConfig firstConfig = new ConfigManager().from(baseProperties());
        try (PMSTestServer server = PMSTestServer.create(firstConfig, schema())) {
            try (PmsTableService first = PmsTableService.open(firstConfig)) {
                first.write(Map.of("id", 1, "marker", "old"));
                first.write(Map.of("id", 2, "marker", "delete-me"));
                first.write(Map.of("id", 3, "marker", "keep"));
                assertEquals(3L, first.freezeCurMemTable().fenceSequenceId());
                first.flushImmutableMemTable();
                sinkAllAvailable(first);
            }
            String firstCommitUser = server.table().latestSnapshot().orElseThrow().commitUser();

            PmsServerConfig freshConfig = new ConfigManager().from(baseProperties(
                tempDir.resolve("fresh"), tempDir.resolve("warehouse")
            ));
            try (PmsTableService fresh = PmsTableService.open(freshConfig)) {
                assertEquals(0L, number(fresh.state(), "lastAssignedSequenceId"));
                fresh.write(Map.of("id", 1, "marker", "updated"));
                fresh.delete(Map.of("id", 2));
                fresh.write(Map.of("id", 4, "marker", "inserted"));
                // The same commit identifier must represent new data for a fresh local state.
                assertEquals(3L, fresh.freezeCurMemTable().fenceSequenceId());
                fresh.flushImmutableMemTable();
                sinkAllAvailable(fresh);
                assertEquals(3L, number(fresh.state(), "lastPersistedSequenceId"));
            }

            assertEquals(Map.of(1, "updated", 3, "keep", 4, "inserted"), server.readIntStringRows());
            String freshCommitUser = server.table().latestSnapshot().orElseThrow().commitUser();
            assertNotEquals(firstCommitUser, freshCommitUser);
            assertTrue(freshCommitUser.matches("pms-server-[0-9a-f]{12}"));
        }
    }

    @Test
    void committedSinkRecoveryKeepsWriterIdentityAndDoesNotCommitTwice() throws Exception {
        PmsServerConfig config = new ConfigManager().from(baseProperties());
        try (PMSTestServer server = PMSTestServer.create(config, schema())) {
            try (PmsTableService first = PmsTableService.open(config, (table, commitUser, storage) ->
                    new SinkManager() {
                        private final SinkManager delegate = new PaimonSinkManager(table, commitUser, storage);

                        @Override
                        public PreparedSinkCommit prepare(SinkBatch batch) {
                            return delegate.prepare(batch);
                        }

                        @Override
                        public SinkCommitResult commit(PreparedSinkCommit prepared) {
                            delegate.commit(prepared);
                            throw new RuntimeException("forced failure after Paimon commit");
                        }
                    })) {
                first.write(Map.of("id", 1, "marker", "committed"));
                first.freezeCurMemTable();
                first.flushImmutableMemTable();
                assertThrows(RuntimeException.class, () -> sinkAllAvailable(first));
            }
            var committedSnapshot = server.table().latestSnapshot().orElseThrow();

            try (PmsTableService recovered = PmsTableService.open(config)) {
                assertEquals(1L, number(recovered.state(), "lastPersistedSequenceId"));
                assertEquals(committedSnapshot.id(), server.table().latestSnapshot().orElseThrow().id());
                recovered.write(Map.of("id", 2, "marker", "after-restart"));
                assertEquals(2L, recovered.freezeCurMemTable().fenceSequenceId());
                recovered.flushImmutableMemTable();
                sinkAllAvailable(recovered);
            }
            var nextSnapshot = server.table().latestSnapshot().orElseThrow();
            assertEquals(committedSnapshot.commitUser(), nextSnapshot.commitUser());
            assertEquals(committedSnapshot.id() + 1, nextSnapshot.id());
            assertEquals(Map.of(1, "committed", 2, "after-restart"), server.readIntStringRows());
        }
    }

    @Test
    void missingFlushedSstFailsStartupInsteadOfDroppingData() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "missing-a"));
            flushAndWait(server);
            server.stop();
        }

        Files.delete(flushedSstPath());

        PmsServerRuntime runtime = new PmsServerRuntime(new ConfigManager().from(baseProperties()));
        Exception error = assertThrows(Exception.class, runtime::start);
        assertTrue(causalMessages(error).contains(
            "Unpersisted flush boundary is not covered by recovered SSTs"
        ));
        assertEquals(PmsRuntimeStatus.FAILED, runtime.status());
    }

    @Test
    void corruptFlushedSstFailsStartupWithDiagnosticError() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "corrupt-a"));
            flushAndWait(server);
            server.stop();
        }

        Files.write(flushedSstPath(), new byte[] {1, 2, 3, 4});

        PmsServerRuntime runtime = new PmsServerRuntime(new ConfigManager().from(baseProperties()));
        Exception error = assertThrows(Exception.class, runtime::start);
        String messages = causalMessages(error);
        assertTrue(messages.contains("Cannot read SST data"));
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
            flushAndWait(server);
            sinkAndWait(server);
            assertEquals(Map.of(1, "old-a"), server.readIntStringRows());

            server.delete(Map.of("id", 1));
            flushAndWait(server);
            sinkAndWait(server);

            assertEquals(Map.of(), server.readIntStringRows());
        }
    }

    @Test
    void getUsesPaimonLookupAfterLocalMissAndTombstoneBlocksHistoryLookup() throws Exception {
        Path warehouse = tempDir.resolve("warehouse");

        PmsServerConfig writerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("writer"), warehouse)
        );
        try (PMSTestServer writer = PMSTestServer.create(writerConfig, schema()).start()) {
            writer.write(Map.of("id", 1, "marker", "paimon-a"));
            flushAndWait(writer);
            sinkAndWait(writer);
            assertEquals(Map.of(1, "paimon-a"), writer.readIntStringRows());
        }

        PmsServerConfig readerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("reader"), warehouse)
        );
        try (PMSTestServer reader = PMSTestServer.create(readerConfig, schema()).start()) {
            assertEquals(
                Map.of("id", 1, "marker", "paimon-a"),
                reader.get(Map.of("id", 1)).orElseThrow()
            );

            reader.delete(Map.of("id", 1));

            assertFalse(reader.get(Map.of("id", 1)).isPresent());
            assertEquals(Type.DELETED, reader.getLocal(Map.of("id", 1)).type());
            assertEquals(Map.of(1, "paimon-a"), reader.readIntStringRows());
        }
    }

    @Test
    void repeatedPaimonLookupBuildsAndUsesValueSstCache() throws Exception {
        Path warehouse = tempDir.resolve("warehouse");

        PmsServerConfig writerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("writer"), warehouse)
        );
        try (PMSTestServer writer = PMSTestServer.create(writerConfig, schema()).start()) {
            writer.write(Map.of("id", 1, "marker", "cache-a"));
            flushAndWait(writer);
            sinkAndWait(writer);
        }

        PmsServerConfig readerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("reader"), warehouse)
        );
        try (PMSTestServer reader = PMSTestServer.create(readerConfig, schema()).start()) {
            for (int i = 0; i < readerConfig.lookup().buildThreshold(); i++) {
                assertEquals(
                    Map.of("id", 1, "marker", "cache-a"),
                    reader.get(Map.of("id", 1)).orElseThrow()
                );
            }

            waitUntil(() -> number(reader.runtime().state(), "lookupCacheBuildsSucceeded") >= 1);

            assertEquals(
                Map.of("id", 1, "marker", "cache-a"),
                reader.get(Map.of("id", 1)).orElseThrow()
            );
            Map<String, Object> state = reader.runtime().state();
            assertTrue(number(state, "lookupLocalLookups") >= 1);
            assertTrue(number(state, "lookupCacheReadyEntries") >= 1);
            assertTrue(number(state, "lookupCacheBytes") > 0);
        }
    }

    @Test
    void getLocalOnlyReadsPmsLocalLayersAndExposesTombstones() throws Exception {
        Path warehouse = tempDir.resolve("warehouse");

        PmsServerConfig writerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("writer"), warehouse)
        );
        try (PMSTestServer writer = PMSTestServer.create(writerConfig, schema()).start()) {
            writer.write(Map.of("id", 1, "marker", "paimon-a"));
            flushAndWait(writer);
            sinkAndWait(writer);
            assertEquals(Map.of(1, "paimon-a"), writer.readIntStringRows());
        }

        PmsServerConfig readerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("reader"), warehouse)
        );
        try (PMSTestServer reader = PMSTestServer.create(readerConfig, schema()).start()) {
            assertEquals(
                Map.of("id", 1, "marker", "paimon-a"),
                reader.get(Map.of("id", 1)).orElseThrow()
            );
            assertEquals(Type.MISS, reader.getLocal(Map.of("id", 1)).type());

            reader.write(Map.of("id", 2, "marker", "local-a"));
            var hit = reader.getLocal(Map.of("id", 2));
            assertEquals(Type.HIT, hit.type());
            assertEquals(Map.of("id", 2, "marker", "local-a"), hit.row());

            reader.delete(Map.of("id", 1));
            assertEquals(Type.DELETED, reader.getLocal(Map.of("id", 1)).type());

            Map<String, Object> missResponse = reader.postJson("/getLocal", "{\"id\":3}");
            assertEquals("MISS", missResponse.get("result"));
            assertEquals(false, missResponse.get("found"));
            assertEquals("PMS_LOCAL", missResponse.get("source"));
        }
    }

    @Test
    void sinkedCountCompactsAllFittingRunsBeforeConsideringOldestEviction() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.storage.sinked_sst.max_count", "2");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "retained-1"));
            flushAndWait(server);
            sinkAndWait(server);

            server.write(Map.of("id", 2, "marker", "retained-2"));
            flushAndWait(server);
            sinkAndWait(server);

            Path oldestSinked = tempDir.resolve("storage").resolve("sst-000001-000001.sst");
            assertTrue(Files.exists(oldestSinked));
            assertEquals(
                Map.of("id", 1, "marker", "retained-1"),
                server.get(Map.of("id", 1)).orElseThrow()
            );

            server.write(Map.of("id", 3, "marker", "retained-3"));
            flushAndWait(server);
            sinkAndWait(server);
            waitUntil(() -> {
                try {
                    return number(server.getJson("/state"), "sinkedSSTCount") == 1;
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> state = server.getJson("/state");
            assertEquals(0L, number(state, "newSSTTotalRows"));
            assertEquals(3L, number(state, "sinkedSSTTotalRows"));
            assertEquals(1L, number(state, "sinkedSSTCount"));
            assertFalse(Files.exists(oldestSinked));
            assertFalse(Files.exists(tempDir.resolve("storage").resolve("sst-000002-000002.sst")));
            assertFalse(Files.exists(tempDir.resolve("storage").resolve("sst-000003-000003.sst")));
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000003.sst")));

            assertEquals(
                Map.of("id", 1, "marker", "retained-1"),
                server.get(Map.of("id", 1)).orElseThrow()
            );
            assertEquals(Map.of(1, "retained-1", 2, "retained-2", 3, "retained-3"), server.readIntStringRows());
        }
    }

    @Test
    void sinkRetentionCompactsAnExplicitSinkedRunGroupBeforeEviction() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.storage.sinked_sst.max_count", "1");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "compact-1"));
            flushAndWait(server);
            server.write(Map.of("id", 2, "marker", "compact-2"));
            flushAndWait(server);

            sinkAndWait(server);
            waitUntil(() -> {
                try {
                    return number(server.getJson("/state"), "sinkedSSTCount") == 1;
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> state = server.getJson("/state");
            assertEquals(0L, number(state, "newSSTCount"));
            assertEquals(1L, number(state, "sinkedSSTCount"));
            assertEquals(2L, number(state, "sinkedSSTTotalRows"));
            assertFalse(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000001.sst")));
            assertFalse(Files.exists(tempDir.resolve("storage").resolve("sst-000002-000002.sst")));
            assertTrue(Files.exists(tempDir.resolve("storage").resolve("sst-000001-000002.sst")));
            assertEquals(
                Map.of("id", 1, "marker", "compact-1"),
                server.getLocal(Map.of("id", 1)).row()
            );
            assertEquals(
                Map.of("id", 2, "marker", "compact-2"),
                server.getLocal(Map.of("id", 2)).row()
            );
        }
    }

    @Test
    void prefixLocalReturnsLatestRowsAcrossLocalLayersAndFiltersTombstones() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, compositePkSchema()).start()) {
            server.write(Map.of("id", 1, "sub_id", 1, "marker", "old-1"));
            server.write(Map.of("id", 1, "sub_id", 2, "marker", "old-2"));
            server.write(Map.of("id", 2, "sub_id", 1, "marker", "outside"));
            flushAndWait(server);
            sinkAndWait(server);

            server.write(Map.of("id", 1, "sub_id", 1, "marker", "new-1"));
            server.delete(Map.of("id", 1, "sub_id", 2));
            flushAndWait(server);

            server.write(Map.of("id", 1, "sub_id", 3, "marker", "cur-3"));

            List<Map<String, Object>> rows = server.prefixLocal(Map.of("id", 1));

            assertEquals(
                List.of(
                    Map.of("id", 1, "sub_id", 1, "marker", "new-1"),
                    Map.of("id", 1, "sub_id", 3, "marker", "cur-3")
                ),
                rows
            );

            Map<String, Object> response = server.postJson("/prefixLocal", "{\"id\":1}");
            assertEquals(2L, number(response, "count"));
            assertEquals("PMS_LOCAL", response.get("source"));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> httpRows = (List<Map<String, Object>>) response.get("rows");
            assertEquals(rows, httpRows);
            var unsupportedPrefixResult = server.postResult("/prefix", "{\"id\":1}");
            Map<String, Object> unsupportedPrefix = unsupportedPrefixResult.jsonObject();
            assertEquals("NOT_SUPPORTED", unsupportedPrefix.get("status"));
            assertEquals(501, unsupportedPrefixResult.statusCode());
            assertEquals(400, server.postResult("/prefixLocal", "{\"sub_id\":1}").statusCode());
        }
    }

    @Test
    void binaryProtocolDrivesRawWriteBatchAndLookupOverHttp2() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            HttpClient client = http2Client();
            PmsHandshake handshake = requireHttp2Handshake(client, server);

            EncodedRow row1 = encodedRow(server.table().rowType(), 1, "proto-a");
            EncodedRow row2 = encodedRow(server.table().rowType(), 2, "proto-b");
            byte[] requestBody = RecordBatchCodec.encodeRequest(List.of(
                RawKvEntry.put(row1.key(), row1.row()),
                RawKvEntry.delete(row1.key()),
                RawKvEntry.put(row2.key(), row2.row())
            ));

            HttpResponse<byte[]> batchResponse = postBinary(client, server, PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, requestBody);
            assertEquals(HttpClient.Version.HTTP_2, batchResponse.version());
            assertEquals(200, batchResponse.statusCode());
            WriteResult writeResult = WriteResultCodec.decodeResponse(batchResponse.body());
            assertEquals(PmsStatus.OK, writeResult.status());
            assertEquals(3, writeResult.acceptedCount());

            RawLookupResult deleted = singleLookup(postBinary(
                client,
                server,
                PmsProtocolConstants.LOCAL_GET_PATH,
                KeyBatchCodec.encodeSingle(row1.key())
            ).body());
            assertEquals(LookupResultType.DELETED, deleted.type());

            RawLookupResult hit = singleLookup(postBinary(
                client,
                server,
                PmsProtocolConstants.FULL_GET_PATH,
                KeyBatchCodec.encodeSingle(row2.key())
            ).body());
            assertEquals(LookupResultType.HIT, hit.type());
            assertArrayEquals(row2.row(), hit.row());

            RawLookupBatchResult prefix = LookupBatchCodec.decodeResponse(postBinary(
                client,
                server,
                PmsProtocolConstants.LOCAL_GET_PREFIX_PATH,
                KeyBatchCodec.encodeSingle(row2.key())
            ).body());
            assertEquals(PmsStatus.OK, prefix.status());
            assertEquals(1, prefix.results().size());
            assertArrayEquals(row2.row(), prefix.results().get(0).row());

            Map<String, Object> jsonState = server.getJson("/state");
            @SuppressWarnings("unchecked")
            Map<String, Object> runtime = (Map<String, Object>) jsonState.get("runtime");
            assertEquals("RUNNING", runtime.get("status"));
        }
    }

    @Test
    void rawClientDrivesBatchWriteAndLookupOverHttp2() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start();
             PmsRawClient client = PmsRawClient.connect(PmsClientConfig.builder(server.baseUri()).build())) {
            assertEquals("pms", client.handshake().backend());

            EncodedRow row1 = encodedRow(server.table().rowType(), 1, "client-a");
            EncodedRow row2 = encodedRow(server.table().rowType(), 2, "client-b");
            try (PmsRawBatchWriter writer = client.newBatchWriter(2)) {
                writer.put(row1.key(), row1.row());
                writer.delete(row1.key());
                writer.put(row2.key(), row2.row());
                assertEquals(1, writer.pendingCount());
            }

            RawLookupResult deleted = client.getLocal(row1.key());
            assertEquals(LookupResultType.DELETED, deleted.type());

            RawLookupResult hit = client.getFull(row2.key());
            assertEquals(LookupResultType.HIT, hit.type());
            assertArrayEquals(row2.row(), hit.row());

            RawLookupBatchResult prefix = client.getPrefixLocal(row2.key());
            assertEquals(PmsStatus.OK, prefix.status());
            assertEquals(1, prefix.results().size());
            assertArrayEquals(row2.row(), prefix.results().get(0).row());
        }
    }

    @Test
    void writeAdmissionReturnsOverloadedAtConfiguredWatermark() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.memtable.max_entries", "1");
        props.setProperty("pms.flowcontrol.overloaded_immutable_count", "1");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start();
             PmsRawClient client = PmsRawClient.connect(
                 PmsClientConfig.builder(server.baseUri()).writeRetryMax(0).build())) {
            EncodedRow row1 = encodedRow(server.table().rowType(), 1, "admitted");
            EncodedRow row2 = encodedRow(server.table().rowType(), 2, "rejected");

            assertEquals(PmsStatus.OK, client.putDetailed(row1.key(), row1.row()).status());
            assertEquals(PmsStatus.OVERLOADED, client.putDetailed(row2.key(), row2.row()).status());
            assertEquals(LookupResultType.HIT, client.getLocal(row1.key()).type());
            assertEquals(LookupResultType.MISS, client.getLocal(row2.key()).type());
            assertEquals(true, server.getJson("/state").get("writeOverloaded"));
        }
    }

    @Test
    void prefixResultCountUsesNegotiatedBatchLimit() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.protocol.max_batch_entries", "1");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start();
             PmsRawClient client = PmsRawClient.connect(PmsClientConfig.builder(server.baseUri()).build())) {
            EncodedRow row1 = encodedRow(server.table().rowType(), 1, "prefix-a");
            EncodedRow row2 = encodedRow(server.table().rowType(), 2, "prefix-b");
            assertEquals(PmsStatus.OK, client.putDetailed(row1.key(), row1.row()).status());
            assertEquals(PmsStatus.OK, client.putDetailed(row2.key(), row2.row()).status());

            RawLookupBatchResult result = client.getPrefixLocal(new byte[0]);

            assertEquals(PmsStatus.OVERLOADED, result.status());
            assertTrue(result.results().isEmpty());
        }
    }

    @Test
    void binaryEndpointRejectsChunkedBodyAboveConfiguredLimit() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.protocol.max_request_body_bytes", "4");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start()) {
            HttpClient client = http2Client();
            requireHttp2Handshake(client, server);
            HttpResponse<byte[]> response = client.send(
                request(server, PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH)
                    .header("content-type", PmsProtocolConstants.CONTENT_TYPE_BINARY)
                    .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream(new byte[5])
                    ))
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray()
            );

            assertEquals(400, response.statusCode());
            assertEquals(PmsStatus.BAD_REQUEST, WriteResultCodec.decodeResponse(response.body()).status());
        }
    }

    @Test
    void fatalWriteFailureMarksRuntimeFailedAndStopsServer() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start()) {
            PmsServerRuntime runtime = server.runtime();

            runtime.failFatalAsync(
                new PmsFatalWriteException("injected fatal write", new IOException("apply failed"))
            );

            waitUntil(() -> runtime.status() == PmsRuntimeStatus.FAILED && httpServerStopped(runtime));
            assertEquals(PmsRuntimeStatus.FAILED, runtime.status());
            @SuppressWarnings("unchecked")
            Map<String, Object> runtimeState = (Map<String, Object>) runtime.state().get("runtime");
            assertEquals("injected fatal write", runtimeState.get("lastFailureMessage"));
        }
    }

    @Test
    void rowClientEncodesRowsAndDecodesLookupsOverHttp2() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema()).start();
             PmsClient client = PmsClient.connect(PmsClientConfig.builder(server.baseUri()).build())) {
            assertEquals("pms", client.handshake().backend());
            assertEquals(server.table().rowType(), client.rowType());
            assertEquals(List.of("id"), client.primaryKeyFieldNames());
            assertNotNull(client.handshake().tableSchema());
            assertEquals(client.handshake().tableSchema().schemaId(), client.writerSchemaId());

            WriteResult write = client.writeBatch(List.of(
                row(RowKind.INSERT, 1, "row-client-a"),
                row(RowKind.DELETE, 1, "ignored-delete-payload"),
                row(RowKind.UPDATE_AFTER, 2, "row-client-b")
            ));
            assertEquals(PmsStatus.OK, write.status());
            assertEquals(3, write.acceptedCount());

            PmsRowLookupResult deleted = client.getLocal(GenericRow.of(1));
            assertEquals(LookupResultType.DELETED, deleted.type());

            PmsRowLookupResult hit = client.get(GenericRow.of(2));
            assertEquals(LookupResultType.HIT, hit.type());
            assertEquals(2, hit.row().getInt(0));
            assertEquals("row-client-b", hit.row().getString(1).toString());

            PmsRowLookupBatchResult prefix = client.prefixLocal(GenericRow.of(2));
            assertEquals(PmsStatus.OK, prefix.status());
            assertEquals(1, prefix.results().size());
            assertEquals("row-client-b", prefix.results().get(0).row().getString(1).toString());
        }
    }

    @Test
    void binaryFullGetFallsThroughToPaimonAfterLocalMiss() throws Exception {
        Path warehouse = tempDir.resolve("warehouse");

        PmsServerConfig writerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("writer"), warehouse)
        );
        try (PMSTestServer writer = PMSTestServer.create(writerConfig, schema()).start()) {
            writer.write(Map.of("id", 1, "marker", "paimon-binary"));
            flushAndWait(writer);
            sinkAndWait(writer);
        }

        PmsServerConfig readerConfig = new ConfigManager().from(
            baseProperties(tempDir.resolve("reader"), warehouse)
        );
        try (PMSTestServer reader = PMSTestServer.create(readerConfig, schema()).start()) {
            HttpClient client = http2Client();
            requireHttp2Handshake(client, reader);
            EncodedRow lookup = encodedRow(reader.table().rowType(), 1, "unused");

            RawLookupResult local = singleLookup(postBinary(
                client,
                reader,
                PmsProtocolConstants.LOCAL_GET_PATH,
                KeyBatchCodec.encodeSingle(lookup.key())
            ).body());
            assertEquals(LookupResultType.MISS, local.type());

            RawLookupResult full = singleLookup(postBinary(
                client,
                reader,
                PmsProtocolConstants.FULL_GET_PATH,
                KeyBatchCodec.encodeSingle(lookup.key())
            ).body());
            assertEquals(LookupResultType.HIT, full.type());
            assertEquals(
                "paimon-binary",
                new PmsRowValueCodec().decode(reader.table().rowType(), full.row()).getString(1).toString()
            );
        }
    }

    @Test
    void schedulerAutomaticallyFlushesAndSinks() throws Exception {
        Properties props = baseProperties();
        props.setProperty("pms.server.scheduler.flush_reconcile_interval_ms", "25");
        props.setProperty("pms.server.scheduler.maintenance_reconcile_interval_ms", "25");
        props.setProperty("pms.paimon.visibility.max_delay_ms", "50");
        PmsServerConfig config = new ConfigManager().from(props);

        try (PMSTestServer server = PMSTestServer.create(config, schema()).start()) {
            server.write(Map.of("id", 1, "marker", "auto-a"));

            waitUntil(() -> {
                try {
                    Map<String, Object> state = server.getJson("/state");
                    return Map.of(1, "auto-a").equals(server.readIntStringRows())
                        && number(state, "lastPersistedSequenceId") >= 1;
                } catch (Exception e) {
                    return false;
                }
            });

            Map<String, Object> state = server.getJson("/state");
            @SuppressWarnings("unchecked")
            Map<String, Object> scheduler = (Map<String, Object>) state.get("scheduler");
            assertEquals(true, scheduler.get("running"));
            assertEquals(25L, number(scheduler, "flushReconcileIntervalMs"));
            assertEquals(25L, number(scheduler, "maintenanceReconcileIntervalMs"));
            assertTrue(scheduler.get("flushRunning") instanceof Boolean);
            assertTrue(scheduler.get("maintenanceRunning") instanceof Boolean);
            assertEquals(0L, number(scheduler, "pendingPaimonFenceSequenceId"));
        }
    }

    @Test
    void serviceFlushCompletesWhileSinkPrepareIsBlocked() throws Exception {
        try (PMSTestServer server = PMSTestServer.create(tempDir, schema())) {
            AtomicReference<BlockingPrepareSinkManager> sinkManagerRef = new AtomicReference<>();
            try (PmsTableService service = PmsTableService.open(
                    server.config(),
                    (table, commitUser, storage) -> {
                        BlockingPrepareSinkManager manager = new BlockingPrepareSinkManager(
                            new PaimonSinkManager(table, commitUser, storage)
                        );
                        sinkManagerRef.set(manager);
                        return manager;
                    })) {
                BlockingPrepareSinkManager sinkManager = sinkManagerRef.get();
                assertNotNull(sinkManager);
                service.write(Map.of("id", 1, "marker", "before-sink"));
                service.freezeCurMemTable();
                service.flushImmutableMemTable();

                AtomicReference<Throwable> sinkFailure = new AtomicReference<>();
                Thread sinkThread = new Thread(() -> {
                    try {
                        sinkAllAvailable(service);
                    } catch (Throwable failure) {
                        sinkFailure.set(failure);
                    }
                });
                AtomicReference<Throwable> flushFailure = new AtomicReference<>();
                Thread flushThread = new Thread(() -> {
                    try {
                        service.freezeCurMemTable();
                        service.flushImmutableMemTable();
                    } catch (Throwable failure) {
                        flushFailure.set(failure);
                    }
                });
                try {
                    sinkThread.start();
                    assertTrue(sinkManager.prepareEntered.await(5, TimeUnit.SECONDS));

                    service.write(Map.of("id", 2, "marker", "during-sink"));
                    flushThread.start();
                    flushThread.join(TimeUnit.SECONDS.toMillis(5));

                    assertFalse(flushThread.isAlive(), "service Flush was blocked by in-flight Sink");
                    assertNull(flushFailure.get());
                } finally {
                    sinkManager.allowPrepare.countDown();
                    sinkThread.join(TimeUnit.SECONDS.toMillis(5));
                    flushThread.join(TimeUnit.SECONDS.toMillis(5));
                }

                assertFalse(sinkThread.isAlive(), "service Sink did not finish");
                assertNull(sinkFailure.get());
                Map<String, Object> afterFirstSink = service.state();
                assertEquals(1L, number(afterFirstSink, "newSSTCount"));
                assertEquals(1L, number(afterFirstSink, "sinkedSSTCount"));

                sinkAllAvailable(service);
                Map<String, Object> afterSecondSink = service.state();
                assertEquals(0L, number(afterSecondSink, "newSSTCount"));
                assertEquals(2L, number(afterSecondSink, "sinkedSSTCount"));
            }
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
        assertTrue(config.coreConfig().paimon().cacheEnabled());
        assertEquals("128mb", config.coreConfig().paimon().manifestCacheSmallFileMemory());
        assertEquals("1mb", config.coreConfig().paimon().manifestCacheSmallFileThreshold());
        assertEquals(600000, config.scheduler().flushReconcileIntervalMs());
        assertEquals(600000, config.scheduler().maintenanceReconcileIntervalMs());
        assertEquals(600000, config.scheduler().visibilityMaxDelayMs());
        assertEquals(20, config.coreConfig().flowcontrol().overloadedPendingSstCount());
        assertTrue(config.protocol().strictHttp2());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_KEY_BYTES, config.protocol().maxKeyBytes());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_ROW_BYTES, config.protocol().maxRowBytes());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_BATCH_ENTRIES, config.protocol().maxBatchEntries());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_CONCURRENT_STREAMS, config.protocol().maxConcurrentStreams());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_REQUEST_BODY_BYTES, config.protocol().maxRequestBodyBytes());
        assertEquals(PmsProtocolConfig.DEFAULT_MAX_RESPONSE_BODY_BYTES, config.protocol().maxResponseBodyBytes());
        assertTrue(config.lookup().cacheEnabled());
        assertEquals(tempDir.resolve("target").resolve("lookup-cache").toAbsolutePath().normalize(), config.lookup().cacheDir());
        assertEquals(3L * 1024 * 1024 * 1024, config.lookup().maxCacheBytes());
        assertEquals(3, config.lookup().buildThreshold());
        assertEquals(2, config.lookup().buildThreads());
        assertEquals(1024, config.lookup().directMetadataCacheEntries());
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
    void configManagerRejectsRemovedSchedulerKeys() {
        for (String removedKey : List.of(
                "pms.server.scheduler.sink_interval_ms",
                "pms.server.scheduler.enabled",
                "pms.server.scheduler.failure_retry_delay_ms",
                "pms.operation.sink.batch_max_ssts")) {
            Properties props = baseProperties();
            props.setProperty(removedKey, "1000");

            IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new ConfigManager().from(props)
            );

            assertTrue(error.getMessage().contains("Removed scheduler config key"));
        }
    }

    @Test
    void configManagerRequiresNewSstTargetBelowWriteHardLimit() {
        Properties props = baseProperties();
        props.setProperty("pms.storage.new_sst.max_count", "10");
        props.setProperty("pms.flowcontrol.overloaded_pending_sst_count", "10");

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(props)
        );

        assertTrue(error.getMessage().contains("new_sst.max_count"));
        assertTrue(error.getMessage().contains("overloaded_pending_sst_count"));
    }

    @Test
    void configManagerRejectsLookupCacheDirectoryOverlappingDurableState() {
        Properties walOverlap = baseProperties();
        walOverlap.setProperty("pms.lookup.cache.dir", tempDir.resolve("wal").resolve("lookup").toString());
        IllegalArgumentException walError = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(walOverlap)
        );
        assertTrue(walError.getMessage().contains("pms.wal.dir"));

        Properties storageOverlap = baseProperties();
        storageOverlap.setProperty("pms.lookup.cache.dir", tempDir.resolve("storage").resolve("lookup").toString());
        IllegalArgumentException storageError = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(storageOverlap)
        );
        assertTrue(storageError.getMessage().contains("pms.storage.dir"));

        Properties warehouseOverlap = baseProperties();
        warehouseOverlap.setProperty("pms.lookup.cache.dir", tempDir.resolve("warehouse").resolve("lookup").toString());
        IllegalArgumentException warehouseError = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(warehouseOverlap)
        );
        assertTrue(warehouseError.getMessage().contains("Paimon warehouse"));
    }

    @Test
    void configManagerParsesLookupCacheOverrides() {
        Properties props = baseProperties();
        props.setProperty("pms.lookup.cache.enabled", "false");
        props.setProperty("pms.lookup.cache.max_bytes", "128mb");
        props.setProperty("pms.lookup.cache.build_threshold", "5");
        props.setProperty("pms.lookup.cache.build_threads", "4");
        props.setProperty("pms.lookup.cache.build_timeout_ms", "1000");
        props.setProperty("pms.lookup.cache.retry_backoff_ms", "2000");
        props.setProperty("pms.lookup.direct.metadata_cache_entries", "64");

        PmsServerConfig config = new ConfigManager().from(props);

        assertFalse(config.lookup().cacheEnabled());
        assertEquals(128L * 1024 * 1024, config.lookup().maxCacheBytes());
        assertEquals(5, config.lookup().buildThreshold());
        assertEquals(4, config.lookup().buildThreads());
        assertEquals(java.time.Duration.ofMillis(1000), config.lookup().buildTimeout());
        assertEquals(java.time.Duration.ofMillis(2000), config.lookup().retryBackoff());
        assertEquals(64, config.lookup().directMetadataCacheEntries());
    }

    @Test
    void configManagerParsesProtocolOverrides() {
        Properties props = baseProperties();
        props.setProperty("pms.protocol.strict_http2", "false");
        props.setProperty("pms.protocol.max_key_bytes", "123");
        props.setProperty("pms.protocol.max_row_bytes", "456");
        props.setProperty("pms.protocol.max_batch_entries", "7");
        props.setProperty("pms.protocol.max_concurrent_streams", "8");
        props.setProperty("pms.protocol.max_request_body_bytes", "999");
        props.setProperty("pms.protocol.max_response_body_bytes", "1001");

        PmsServerConfig config = new ConfigManager().from(props);

        assertFalse(config.protocol().strictHttp2());
        assertEquals(123, config.protocol().maxKeyBytes());
        assertEquals(456, config.protocol().maxRowBytes());
        assertEquals(7, config.protocol().maxBatchEntries());
        assertEquals(8, config.protocol().maxConcurrentStreams());
        assertEquals(999, config.protocol().maxRequestBodyBytes());
        assertEquals(1001, config.protocol().maxResponseBodyBytes());
    }

    @Test
    void configManagerRejectsProtocolBatchLimitAboveCoreLimit() {
        Properties props = baseProperties();
        props.setProperty(
            "pms.protocol.max_batch_entries",
            Integer.toString(PMSBucketDirector.MAX_WRITE_BATCH_COUNT + 1)
        );

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigManager().from(props)
        );

        assertTrue(error.getMessage().contains("exceeds core limit"));
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

    private static Schema compositePkSchema() {
        return Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("sub_id", DataTypes.INT())
            .column("marker", DataTypes.STRING())
            .primaryKey("id", "sub_id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("merge-engine", "deduplicate")
            .build();
    }

    private static void assertJson(String json, String expected) {
        assertTrue(json.contains(expected), () -> "Expected " + expected + " in " + json);
    }

    private static HttpClient http2Client() {
        return HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .proxy(HttpClient.Builder.NO_PROXY)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    private static HttpRequest.Builder request(PMSTestServer server, String path) {
        return HttpRequest.newBuilder(server.baseUri().resolve(path))
            .version(HttpClient.Version.HTTP_2);
    }

    private static PmsHandshake requireHttp2Handshake(HttpClient client, PMSTestServer server) throws Exception {
        HttpResponse<String> response = client.send(
            request(server, PmsProtocolConstants.HANDSHAKE_PATH).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        PmsHandshake handshake = PmsHandshake.fromJson(response.body());
        handshake.requireCompatible();
        assertEquals("pms", handshake.backend());
        assertNotNull(handshake.tableSchema());
        handshake.tableSchema().requireHashMatches();
        return handshake;
    }

    private static HttpResponse<byte[]> postBinary(
            HttpClient client,
            PMSTestServer server,
            String path,
            byte[] body) throws Exception {
        return client.send(
            request(server, path)
                .header("content-type", PmsProtocolConstants.CONTENT_TYPE_BINARY)
                .header("accept", PmsProtocolConstants.CONTENT_TYPE_BINARY)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build(),
            HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private static RawLookupResult singleLookup(byte[] body) {
        RawLookupBatchResult result = LookupBatchCodec.decodeResponse(body);
        assertEquals(PmsStatus.OK, result.status());
        assertEquals(1, result.results().size());
        return result.results().get(0);
    }

    private static EncodedRow encodedRow(RowType rowType, int id, String marker) {
        GenericRow row = row(RowKind.INSERT, id, marker);
        PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id"));
        PmsRowValueCodec valueCodec = new PmsRowValueCodec();
        return new EncodedRow(keyCodec.encodeKey(row), valueCodec.encode(rowType, row, 0));
    }

    private static GenericRow row(RowKind kind, int id, String marker) {
        GenericRow row = new GenericRow(kind, 2);
        row.setField(0, id);
        row.setField(1, BinaryString.fromString(marker));
        return row;
    }

    private Properties baseProperties() {
        return baseProperties(tempDir, tempDir.resolve("warehouse"));
    }

    private Properties baseProperties(Path rootDir, Path warehouse) {
        Properties props = new Properties();
        props.setProperty("pms.server.host", "127.0.0.1");
        props.setProperty("pms.server.port", "0");
        props.setProperty("pms.paimon.warehouse", warehouse.toUri().toString());
        props.setProperty("pms.paimon.database", "pms_db");
        props.setProperty("pms.paimon.table", "server_pk");
        props.setProperty("pms.wal.dir", rootDir.resolve("wal").toString());
        props.setProperty("pms.storage.dir", rootDir.resolve("storage").toString());
        props.setProperty("pms.lookup.cache.dir", rootDir.resolve("target").resolve("lookup-cache").toString());
        props.setProperty("pms.server.scheduler.flush_reconcile_interval_ms", "600000");
        props.setProperty("pms.server.scheduler.maintenance_reconcile_interval_ms", "600000");
        return props;
    }

    private Path flushedSstPath() {
        return tempDir.resolve("storage").resolve("sst-000001-000001.sst");
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

    private static long flushAndWait(PMSTestServer server) throws Exception {
        long fenceSequenceId = server.flush();
        waitUntil(() -> boundaryReached(server, "lastFlushedSequenceId", fenceSequenceId));
        return fenceSequenceId;
    }

    private static long sinkAndWait(PMSTestServer server) throws Exception {
        long fenceSequenceId = server.sink();
        waitUntil(() -> boundaryReached(server, "lastPersistedSequenceId", fenceSequenceId));
        return fenceSequenceId;
    }

    private static boolean boundaryReached(PMSTestServer server, String boundary, long fenceSequenceId) {
        try {
            return number(server.getJson("/state"), boundary) >= fenceSequenceId;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static boolean boundaryReached(PmsServerRuntime runtime, String boundary, long fenceSequenceId) {
        try {
            return number(runtime.state(), boundary) >= fenceSequenceId;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static SinkFlightSnapshot.Status sinkFlight(PmsServerRuntime runtime) {
        return ((SinkFlightSnapshot) runtime.state().get("sinkFlight")).status();
    }

    private static boolean httpServerStopped(PmsServerRuntime runtime) {
        try {
            runtime.port();
            return false;
        } catch (IllegalStateException expected) {
            return true;
        }
    }

    private static long number(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).longValue();
    }

    private static void sinkAllAvailable(PmsTableService service) {
        long targetSequenceId = service.stateSnapshot().newSSTMaxSequenceId();
        if (targetSequenceId > 0) {
            service.sinkToPaimon(new SinkSelection(targetSequenceId, Long.MAX_VALUE));
        }
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

    private static final class FailOnceAfterPrepareSinkManager implements SinkManager {
        private final SinkManager delegate;
        private volatile int prepareCalls;
        private volatile int commitCalls;
        private volatile PreparedSinkCommit firstCommit;
        private volatile PreparedSinkCommit secondCommit;

        private FailOnceAfterPrepareSinkManager(SinkManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) {
            prepareCalls++;
            return delegate.prepare(batch);
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            commitCalls++;
            if (firstCommit == null) {
                firstCommit = prepared;
                throw new RuntimeException("forced first commit failure after prepare");
            }
            secondCommit = prepared;
            return delegate.commit(prepared);
        }
    }

    private static final class BlockingPrepareSinkManager implements SinkManager {
        private final SinkManager delegate;
        private final CountDownLatch prepareEntered = new CountDownLatch(1);
        private final CountDownLatch allowPrepare = new CountDownLatch(1);

        private BlockingPrepareSinkManager(SinkManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedSinkCommit prepare(SinkBatch batch) {
            prepareEntered.countDown();
            try {
                if (!allowPrepare.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting to release blocked Sink prepare");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("blocked Sink prepare was interrupted", e);
            }
            return delegate.prepare(batch);
        }

        @Override
        public SinkCommitResult commit(PreparedSinkCommit prepared) {
            return delegate.commit(prepared);
        }
    }

    private record EncodedRow(byte[] key, byte[] row) {}
}
