package org.qwh.pms.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsSqlIntegrationTest {

    @Test
    void serviceDiscoveryFindsBothFactoryRoles() {
        ClassLoader classLoader = getClass().getClassLoader();

        assertInstanceOf(
                PmsDynamicTableFactory.class,
                FactoryUtil.discoverFactory(
                        classLoader, DynamicTableSinkFactory.class, "pms"));
        assertInstanceOf(
                PmsDynamicTableFactory.class,
                FactoryUtil.discoverFactory(
                        classLoader, DynamicTableSourceFactory.class, "pms"));
    }

    @Test
    void flinkPlannerBuildsSinkPlanAndExecutesPrimaryKeyDelete() throws Exception {
        try (TestingPmsServer server =
                new TestingPmsServer(
                        PmsFlinkTestUtils.simplePaimonRowType(),
                        java.util.List.of("id"))) {
            server.putRow(
                    GenericRow.of(1, BinaryString.fromString("delete-me")));
            TableEnvironment tableEnvironment =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance()
                                    .inBatchMode()
                                    .build());
            tableEnvironment.executeSql(createTableDdl(server, "pms_dim"));

            String insertPlan =
                    tableEnvironment.explainSql(
                            "INSERT INTO pms_dim VALUES (2, 'planned')");
            assertTrue(insertPlan.contains("pms_dim"));

            tableEnvironment
                    .executeSql("DELETE FROM pms_dim WHERE id = 1")
                    .await();

            assertTrue(server.isDeletedKeyTuple(GenericRow.of(1)));
            assertEquals(1, server.deleteRequests());
            assertEquals(0, server.lookupRequests());
        }
    }

    @Test
    void flinkPlannerRejectsDeleteThatWouldRequireScanningOldRows()
            throws Exception {
        try (TestingPmsServer server =
                new TestingPmsServer(
                        PmsFlinkTestUtils.simplePaimonRowType(),
                        java.util.List.of("id"))) {
            TableEnvironment tableEnvironment =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance()
                                    .inBatchMode()
                                    .build());
            tableEnvironment.executeSql(createTableDdl(server, "pms_reject"));

            assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                            tableEnvironment
                                    .executeSql(
                                            "DELETE FROM pms_reject WHERE marker = 'x'")
                                    .await());
            assertEquals(0, server.deleteRequests());
            assertEquals(0, server.lookupRequests());
        }
    }

    @Test
    void flinkPlannerRejectsOrdinaryTableScan() throws Exception {
        try (TestingPmsServer server =
                new TestingPmsServer(
                        PmsFlinkTestUtils.simplePaimonRowType(),
                        java.util.List.of("id"))) {
            TableEnvironment tableEnvironment =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance()
                                    .inBatchMode()
                                    .build());
            tableEnvironment.executeSql(createTableDdl(server, "pms_no_scan"));

            assertThrows(
                    TableException.class,
                    () -> tableEnvironment.explainSql("SELECT * FROM pms_no_scan"));
            assertEquals(0, server.lookupRequests());
        }
    }

    private static String createTableDdl(
            TestingPmsServer server, String tableName) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + "id INT NOT NULL, "
                + "marker STRING, "
                + "PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'pms', "
                + "'endpoint' = '"
                + server.endpoint()
                + "', "
                + "'client.require-http2' = 'false', "
                + "'sink.write-retry.initial-backoff' = '0 ms', "
                + "'sink.write-retry.max-backoff' = '0 ms'"
                + ")";
    }
}
