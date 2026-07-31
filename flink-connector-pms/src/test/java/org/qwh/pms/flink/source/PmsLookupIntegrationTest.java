package org.qwh.pms.flink.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTestUtils;
import org.qwh.pms.flink.TestingPmsServer;
import org.qwh.pms.protocol.api.PmsStatus;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsLookupIntegrationTest {

    @Test
    void synchronousLookupHandlesHitMissNullAndRetry() throws Exception {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        try (TestingPmsServer server =
                new TestingPmsServer(
                        PmsFlinkTestUtils.simplePaimonRowType(),
                        schema.primaryKeyNames())) {
            server.putRow(
                    GenericRow.of(1, BinaryString.fromString("hit")));
            PmsConnectorConfig config =
                    PmsFlinkTestUtils.connectorConfig(
                            server.endpoint().toString(), false);
            PmsLookupFunction function =
                    new PmsLookupFunction(
                            config,
                            schema,
                            PmsLookupPlan.create(new int[][] {{0}}, schema));
            function.open(functionContext());

            assertEquals("hit", marker(function.lookup(GenericRowData.of(1))));
            assertTrue(function.lookup(GenericRowData.of(2)).isEmpty());

            int requestsBeforeNull = server.lookupRequests();
            assertTrue(
                    function.lookup(GenericRowData.of((Object) null)).isEmpty());
            assertEquals(requestsBeforeNull, server.lookupRequests());

            server.tombstoneKeyTuple(GenericRow.of(1));
            assertTrue(function.lookup(GenericRowData.of(1)).isEmpty());
            server.putRow(
                    GenericRow.of(1, BinaryString.fromString("hit")));

            server.enqueueLookupStatus(PmsStatus.LOOKUP_UNAVAILABLE);
            assertEquals("hit", marker(function.lookup(GenericRowData.of(1))));
            assertEquals(requestsBeforeNull + 3, server.lookupRequests());
            function.close();
        }
    }

    @Test
    void asynchronousLookupUsesWorkerOwnedClients() throws Exception {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        try (TestingPmsServer server =
                new TestingPmsServer(
                        PmsFlinkTestUtils.simplePaimonRowType(),
                        schema.primaryKeyNames())) {
            server.putRow(
                    GenericRow.of(7, BinaryString.fromString("async-hit")));
            PmsConnectorConfig config =
                    PmsFlinkTestUtils.connectorConfig(
                            server.endpoint().toString(), true);
            PmsAsyncLookupFunction function =
                    new PmsAsyncLookupFunction(
                            config,
                            schema,
                            PmsLookupPlan.create(new int[][] {{0}}, schema));
            function.open(functionContext());

            Collection<RowData> hit =
                    function.asyncLookup(GenericRowData.of(7))
                            .get(10, TimeUnit.SECONDS);
            Collection<RowData> miss =
                    function.asyncLookup(GenericRowData.of(8))
                            .get(10, TimeUnit.SECONDS);

            assertEquals("async-hit", marker(hit));
            assertTrue(miss.isEmpty());
            function.close();
        }
    }

    private static FunctionContext functionContext() {
        return new FunctionContext(
                null,
                PmsLookupIntegrationTest.class.getClassLoader(),
                null);
    }

    private static String marker(Collection<RowData> rows) {
        assertEquals(1, rows.size());
        return rows.iterator().next().getString(1).toString();
    }
}
