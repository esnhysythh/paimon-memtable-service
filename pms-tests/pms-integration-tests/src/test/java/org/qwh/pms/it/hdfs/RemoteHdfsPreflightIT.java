package org.qwh.pms.it.hdfs;

import org.junit.jupiter.api.Test;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;
import org.qwh.pms.testkit.paimon.PaimonTestTable;
import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;
import org.qwh.pms.testkit.run.PmsTestRun;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteHdfsPreflightIT {

    @Test
    void hdfsAndPaimonCatalogAreUsable() throws Exception {
        PmsTestEnvironment environment = RemoteHdfsTestSupport.environment();
        try (PmsTestRun run = PmsTestRun.create(environment, "preflight")) {
            run.prepareRemote();
            run.verifyRemoteRoundTrip();

            PaimonTestTableSpec spec = PaimonTestTableSpec.forRun(run.runId(), "preflight");
            PaimonTestTable table = run.own(
                PaimonTestTable.create(environment, run.warehouseUri(), spec)
            );

            assertEquals(spec.identifier().getFullName(), table.table().fullName());
            assertTrue(table.table().primaryKeys().contains("id"));
            run.markSuccessful();
        }
    }
}
