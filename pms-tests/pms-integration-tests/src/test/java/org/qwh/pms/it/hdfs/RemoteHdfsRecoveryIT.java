package org.qwh.pms.it.hdfs;

import org.junit.jupiter.api.Test;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.testkit.client.PmsAdminClient;
import org.qwh.pms.testkit.data.DeterministicDataGenerator;
import org.qwh.pms.testkit.data.TestDataSet;
import org.qwh.pms.testkit.data.TestRecord;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;
import org.qwh.pms.testkit.paimon.PaimonTestTable;
import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;
import org.qwh.pms.testkit.paimon.PaimonVerifier;
import org.qwh.pms.testkit.process.PmsProcess;
import org.qwh.pms.testkit.process.PmsProcessConfig;
import org.qwh.pms.testkit.run.PmsTestRun;

import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.qwh.pms.it.hdfs.RemoteHdfsTestSupport.number;

class RemoteHdfsRecoveryIT {
    private static final int RECORD_COUNT = 500;
    private static final int PAYLOAD_SIZE = 96;
    private static final long SEED = 0x5245434F56455259L;

    @Test
    void acknowledgedWalAndLocalChangesSurviveForcedTermination() throws Exception {
        PmsTestEnvironment environment = RemoteHdfsTestSupport.environment();
        TestDataSet data = DeterministicDataGenerator.standardScenario(
            RECORD_COUNT,
            PAYLOAD_SIZE,
            SEED
        );
        Map<Long, TestRecord> initialState = new LinkedHashMap<>();
        data.inserts().forEach(record -> initialState.put(record.id(), record));

        try (PmsTestRun run = PmsTestRun.create(environment, "recovery")) {
            run.prepareRemote();
            PaimonTestTableSpec spec = PaimonTestTableSpec.forRun(run.runId(), "recovery");
            run.recordMetadata(Map.of(
                "database", spec.database(),
                "table", spec.table(),
                "seed", SEED,
                "recordCount", RECORD_COUNT,
                "payloadSize", PAYLOAD_SIZE
            ));
            run.own(PaimonTestTable.create(environment, run.warehouseUri(), spec));
            PmsProcessConfig processConfig = PmsProcessConfig.create(run, "recovery", spec);

            PmsProcess first = run.own(PmsProcess.start(environment, processConfig, run.runId()));
            try (PmsClient client = RemoteHdfsTestSupport.connect(first, environment)) {
                RemoteHdfsTestSupport.writeRecords(client, data.inserts());
            }
            String identity = Files.readString(processConfig.phaseRoot().resolve("storage/commit-user"));
            checkpoint(run, first, environment, "before-first-kill", RECORD_COUNT, 0, 0);
            assertEquals(Map.of(), PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec));
            first.forceKill();

            PmsProcess recovered = run.own(PmsProcess.start(environment, processConfig, run.runId()));
            assertRecovery(run, recovered, environment, "wal-recovery", RECORD_COUNT, 0, RECORD_COUNT);
            assertEquals(identity, Files.readString(processConfig.phaseRoot().resolve("storage/commit-user")));
            try (PmsClient client = RemoteHdfsTestSupport.connect(recovered, environment)) {
                RemoteHdfsTestSupport.assertAllFull(client, initialState, java.util.List.of());
                RemoteHdfsTestSupport.assertAllLocal(client, initialState, java.util.List.of());
            }
            admin(run, recovered, environment).sinkAndAwait();
            assertEquals(
                initialState,
                PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec)
            );

            try (PmsClient client = RemoteHdfsTestSupport.connect(recovered, environment)) {
                RemoteHdfsTestSupport.writeRecords(client, data.updates());
                RemoteHdfsTestSupport.deleteRecords(client, data.deletes());
                RemoteHdfsTestSupport.assertAllFull(client, data.expected(), data.deletes());
            }
            long changeCount = data.updates().size() + data.deletes().size();
            long finalSequence = RECORD_COUNT + changeCount;
            checkpoint(run, recovered, environment, "before-changes-kill", finalSequence, RECORD_COUNT, RECORD_COUNT);
            assertEquals(initialState, PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec));
            recovered.forceKill();

            PmsProcess changesRecovered = run.own(
                PmsProcess.start(environment, processConfig, run.runId())
            );
            assertRecovery(run, changesRecovered, environment, "changes-recovery", changeCount, RECORD_COUNT, finalSequence);
            assertEquals(identity, Files.readString(processConfig.phaseRoot().resolve("storage/commit-user")));
            try (PmsClient client = RemoteHdfsTestSupport.connect(changesRecovered, environment)) {
                RemoteHdfsTestSupport.assertAllFull(client, data.expected(), data.deletes());
                RemoteHdfsTestSupport.assertAllLocal(client, data.expected(), data.deletes());
            }
            Map<String, Object> recoveryState = admin(run, changesRecovered, environment).state();
            assertEquals(
                "RUNNING",
                RemoteHdfsTestSupport.nestedMap(recoveryState, "runtime").get("status")
            );
            admin(run, changesRecovered, environment).sinkAndAwait();
            assertEquals(
                data.expected(),
                PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec)
            );
            long persistedSnapshotId = PaimonVerifier.latestSnapshotId(
                environment,
                run.warehouseUri(),
                spec
            );

            checkpoint(run, changesRecovered, environment, "before-persisted-kill", finalSequence, finalSequence, finalSequence);
            changesRecovered.forceKill();
            PmsProcess persistedRestart = run.own(
                PmsProcess.start(environment, processConfig, run.runId())
            );
            assertRecovery(run, persistedRestart, environment, "persisted-recovery", 0, finalSequence, finalSequence);
            assertEquals(identity, Files.readString(processConfig.phaseRoot().resolve("storage/commit-user")));
            try (PmsClient client = RemoteHdfsTestSupport.connect(persistedRestart, environment)) {
                RemoteHdfsTestSupport.assertAllFull(client, data.expected(), data.deletes());
            }
            persistedRestart.stop();
            assertEquals(
                data.expected(),
                PaimonVerifier.readCurrentRows(environment, run.warehouseUri(), spec)
            );
            assertEquals(
                persistedSnapshotId,
                PaimonVerifier.latestSnapshotId(environment, run.warehouseUri(), spec),
                "A restart and historical reads must not create a new Paimon snapshot"
            );
            run.markSuccessful();
        }
    }

    private static void checkpoint(PmsTestRun run, PmsProcess process, PmsTestEnvironment environment,
            String stage, long assigned, long flushed, long persisted) throws Exception {
        Map<String, Object> state = admin(run, process, environment).state();
        run.recordMetadata(Map.of(stage, state));
        assertEquals(assigned, number(state, "lastAssignedSequenceId"));
        assertEquals(flushed, number(state, "lastFlushedSequenceId"), "WAL-only precondition");
        assertEquals(persisted, number(state, "lastPersistedSequenceId"));
    }

    private static void assertRecovery(PmsTestRun run, PmsProcess process, PmsTestEnvironment environment,
            String stage, long replayed, long flushed, long assigned) throws Exception {
        Map<String, Object> state = admin(run, process, environment).state();
        run.recordMetadata(Map.of(stage, state));
        Map<String, Object> runtime = RemoteHdfsTestSupport.nestedMap(state, "runtime");
        assertEquals("RUNNING", runtime.get("status"));
        Map<String, Object> recovery = RemoteHdfsTestSupport.nestedMap(runtime, "recovery");
        assertEquals(replayed, number(recovery, "recoveredDataRecords"));
        assertEquals(flushed, number(recovery, "lastFlushedSequenceId"));
        assertEquals(0, number(recovery, "pendingPreparedSinkCount"));
        assertEquals(assigned, number(state, "lastAssignedSequenceId"));
    }

    private static PmsAdminClient admin(
            PmsTestRun run,
            PmsProcess process,
            PmsTestEnvironment environment) {
        return new PmsAdminClient(
            process.baseUri(),
            environment.operationTimeout(),
            run.localRunRoot().resolve("last-state.json")
        );
    }
}
