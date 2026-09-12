package org.qwh.pms.it.hdfs;

import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.testkit.client.PmsAdminClient;
import org.qwh.pms.testkit.data.DeterministicDataGenerator;
import org.qwh.pms.testkit.data.TestDataSet;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;
import org.qwh.pms.testkit.paimon.PaimonTestTable;
import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;
import org.qwh.pms.testkit.paimon.PaimonVerifier;
import org.qwh.pms.testkit.process.PmsProcess;
import org.qwh.pms.testkit.process.PmsProcessConfig;
import org.qwh.pms.testkit.run.PmsTestRun;

import java.nio.file.Files;
import java.util.LinkedHashMap;
import org.qwh.pms.testkit.data.TestRecord;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteHdfsSmokeIT {
    private static final int RECORD_COUNT = 1_000;
    private static final int PAYLOAD_SIZE = 128;
    private static final long SEED = 0x504D534954L;

    @Test
    void writeFlushSinkAndHistoricalLookup() throws Exception {
        PmsTestEnvironment environment = RemoteHdfsTestSupport.environment();
        TestDataSet data = DeterministicDataGenerator.standardScenario(
            RECORD_COUNT,
            PAYLOAD_SIZE,
            SEED
        );
        try (PmsTestRun run = PmsTestRun.create(environment, "smoke")) {
            run.prepareRemote();
            PaimonTestTableSpec spec = PaimonTestTableSpec.forRun(run.runId(), "smoke");
            run.recordMetadata(Map.of(
                "database", spec.database(),
                "table", spec.table(),
                "seed", SEED,
                "recordCount", RECORD_COUNT,
                "payloadSize", PAYLOAD_SIZE
            ));
            run.own(PaimonTestTable.create(environment, run.warehouseUri(), spec));

            PmsProcess writer = run.own(PmsProcess.start(
                environment,
                PmsProcessConfig.create(run, "writer", spec),
                run.runId()
            ));
            try (PmsClient client = RemoteHdfsTestSupport.connect(writer, environment)) {
                assertEquals(List.of("id", "payload", "version"), client.rowType().getFieldNames());
                assertEquals(spec.schema().primaryKeys(), client.primaryKeyFieldNames());

                RemoteHdfsTestSupport.writeRecords(client, data.inserts());
                RemoteHdfsTestSupport.writeRecords(client, data.updates());
                RemoteHdfsTestSupport.deleteRecords(client, data.deletes());
                RemoteHdfsTestSupport.assertAllLocal(client, data.expected(), data.deletes());
                RemoteHdfsTestSupport.assertAllFull(client, data.expected(), data.deletes());
                assertEquals(
                    LookupResultType.MISS,
                    client.get(GenericRow.of((long) RECORD_COUNT + 100)).type()
                );
            }

            PmsAdminClient admin = new PmsAdminClient(
                writer.baseUri(),
                environment.operationTimeout(),
                run.localRunRoot().resolve("last-state.json")
            );
            Map<String, Object> flushed = admin.flushAndAwait();
            assertTrue(((Number) flushed.get("lastFlushedSequenceId")).longValue() > 0);
            assertTrue(((Number) flushed.get("newSSTCount")).intValue() > 0);

            Map<String, Object> sinked = admin.sinkAndAwait();
            assertEquals(
                ((Number) sinked.get("lastAssignedSequenceId")).longValue(),
                ((Number) sinked.get("lastPersistedSequenceId")).longValue()
            );
            assertFalse(RemoteHdfsTestSupport.nestedMap(sinked, "runtime").isEmpty());
            assertEquals(
                "RUNNING",
                RemoteHdfsTestSupport.nestedMap(sinked, "runtime").get("status")
            );
            assertEquals(0, ((Number) sinked.get("newSSTCount")).intValue());
            assertTrue(((Number) sinked.get("sinkedSSTCount")).intValue() > 0);
            assertEquals(
                data.expected(),
                PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec)
            );

            run.recordMetadata(Map.of("sinked", sinked));
            String writerIdentity = Files.readString(run.phaseRoot("writer").resolve("storage/commit-user"));
            writer.stop();
            PmsProcess historical = run.own(PmsProcess.start(
                environment,
                PmsProcessConfig.create(run, "historical", spec),
                run.runId()
            ));
            try (PmsClient client = RemoteHdfsTestSupport.connect(historical, environment)) {
                RemoteHdfsTestSupport.assertHistoricalSample(
                    client,
                    data.expected(),
                    data.deletes(),
                    50
                );
            }
            // A new local state resets its sequence, but must commit under a new identity.
            String newIdentity = Files.readString(run.phaseRoot("historical").resolve("storage/commit-user"));
            assertNotEquals(writerIdentity, newIdentity);
            TestRecord updated = new TestRecord(0, "fresh-state-update", 2);
            TestRecord inserted = new TestRecord(RECORD_COUNT + 1, "fresh-state-insert", 0);
            Map<Long, TestRecord> freshExpected = new LinkedHashMap<>(data.expected());
            freshExpected.put(updated.id(), updated);
            freshExpected.put(inserted.id(), inserted);
            freshExpected.remove(1L);
            try (PmsClient client = RemoteHdfsTestSupport.connect(historical, environment)) {
                RemoteHdfsTestSupport.writeRecords(client, List.of(updated, inserted));
                RemoteHdfsTestSupport.deleteRecords(client, List.of(1L));
                RemoteHdfsTestSupport.assertAllFull(client, freshExpected, List.of(1L));
            }
            Map<String, Object> freshSink = new PmsAdminClient(historical.baseUri(),
                environment.operationTimeout(), run.localRunRoot().resolve("fresh-state.json")).sinkAndAwait();
            assertEquals(3, RemoteHdfsTestSupport.number(freshSink, "lastAssignedSequenceId"));
            assertEquals(3, RemoteHdfsTestSupport.number(freshSink, "lastPersistedSequenceId"));
            run.recordMetadata(Map.of("writerIdentity", writerIdentity, "freshWriterIdentity", newIdentity,
                "freshSinked", freshSink));
            assertEquals(freshExpected, PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec));
            historical.stop();
            run.markSuccessful();
        }
    }
}
