package org.qwh.pms.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.server.PmsLocalLookupResult;
import org.qwh.pms.server.dev.PMSTestServer;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink 1.20 本地 MiniCluster、真实 PMS Server 和本地 Paimon 表的端到端冒烟测试.
 */
class PmsRealServerEndToEndTest {

    @TempDir Path tempDir;

    @Test
    void sqlSinkLookupAndDeleteReachRealPmsAndPaimon() throws Exception {
        try (PMSTestServer server =
                PMSTestServer.create(tempDir, paimonSchema()).start()) {
            StreamTableEnvironment sinkEnvironment = tableEnvironment();
            sinkEnvironment.executeSql(createPmsTable(server, "pms_dim"));

            sinkEnvironment
                    .executeSql(
                            "INSERT INTO pms_dim VALUES "
                                    + "(1, 'old'), (2, 'second'), (1, 'latest')")
                    .await();

            assertEquals(
                    Map.of("id", 1, "marker", "latest"),
                    server.get(Map.of("id", 1)).orElseThrow());
            assertEquals(
                    Map.of("id", 2, "marker", "second"),
                    server.get(Map.of("id", 2)).orElseThrow());

            Map<Integer, String> expectedLookup = new LinkedHashMap<>();
            expectedLookup.put(10, "latest");
            expectedLookup.put(11, null);
            assertEquals(expectedLookup, lookupWithMiniCluster(server));

            flushAndSink(server);
            assertEquals(
                    Map.of(1, "latest", 2, "second"),
                    server.readIntStringRows());

            TableEnvironment deleteEnvironment =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance()
                                    .inBatchMode()
                                    .build());
            deleteEnvironment.executeSql(
                    createPmsTable(server, "pms_delete"));
            deleteEnvironment
                    .executeSql("DELETE FROM pms_delete WHERE id = 1")
                    .await();
            assertFalse(server.get(Map.of("id", 1)).isPresent());
            assertEquals(
                    PmsLocalLookupResult.Type.DELETED,
                    server.getLocal(Map.of("id", 1)).type());

            flushAndSink(server);
            assertEquals(Map.of(2, "second"), server.readIntStringRows());
        }
    }

    private static Map<Integer, String> lookupWithMiniCluster(PMSTestServer server)
            throws Exception {
        // 使用显式 DataStream 环境注册 processing-time fact 表.
        StreamExecutionEnvironment streamEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnvironment.setParallelism(2);
        StreamTableEnvironment lookupEnvironment =
                StreamTableEnvironment.create(
                        streamEnvironment,
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .build());
        lookupEnvironment.executeSql(createPmsTable(server, "pms_lookup"));

        DataStream<Row> orders =
                streamEnvironment.fromCollection(
                        List.of(Row.of(10, 1), Row.of(11, 99)),
                        Types.ROW_NAMED(
                                new String[] {"order_id", "id"},
                                Types.INT,
                                Types.INT));
        lookupEnvironment.createTemporaryView(
                "orders",
                orders,
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build());

        Table result =
                lookupEnvironment.sqlQuery(
                        "SELECT o.order_id, d.marker "
                                + "FROM orders AS o "
                                + "LEFT JOIN pms_lookup "
                                + "FOR SYSTEM_TIME AS OF o.proc_time AS d "
                                + "ON o.id = d.id");
        Map<Integer, String> rows = new LinkedHashMap<>();
        try (CloseableIterator<Row> iterator = result.execute().collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                rows.put(
                        (Integer) row.getField(0),
                        (String) row.getField(1));
            }
        }
        return rows;
    }

    private static StreamTableEnvironment tableEnvironment() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.setParallelism(2);
        return StreamTableEnvironment.create(
                environment,
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static String createPmsTable(
            PMSTestServer server, String tableName) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + "id INT NOT NULL, "
                + "marker STRING, "
                + "PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'pms', "
                + "'endpoint' = '"
                + server.baseUri()
                + "', "
                + "'sink.flush.interval' = '0 ms'"
                + ")";
    }

    private static org.apache.paimon.schema.Schema paimonSchema() {
        return org.apache.paimon.schema.Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.INT())
                .column("marker", org.apache.paimon.types.DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .option("file.format", "parquet")
                .option("merge-engine", "deduplicate")
                .build();
    }

    private static void flushAndSink(PMSTestServer server) throws Exception {
        long flushFence = server.flush();
        waitForBoundary(server, "lastFlushedSequenceId", flushFence);
        long sinkFence = server.sink();
        waitForBoundary(server, "lastPersistedSequenceId", sinkFence);
    }

    private static void waitForBoundary(
            PMSTestServer server, String boundary, long fence) throws Exception {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Object value = server.getJson("/state").get(boundary);
            if (value instanceof Number number && number.longValue() >= fence) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError(
                "PMS boundary 未在超时前到达: " + boundary + " >= " + fence);
    }
}
