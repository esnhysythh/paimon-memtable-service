package org.qwh.pms.it.hdfs;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.RowKind;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.client.PmsClientConfig;
import org.qwh.pms.client.PmsRowLookupResult;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.WriteResult;
import org.qwh.pms.testkit.data.TestRecord;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;
import org.qwh.pms.testkit.process.PmsProcess;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

final class RemoteHdfsTestSupport {
    private static final int BATCH_SIZE = 256;

    private RemoteHdfsTestSupport() {}

    static PmsTestEnvironment environment() {
        return PmsTestEnvironment.fromSystemProperties();
    }

    static PmsClient connect(PmsProcess process, PmsTestEnvironment environment) {
        return PmsClient.connect(PmsClientConfig.builder(process.baseUri())
            .connectTimeout(environment.startTimeout())
            .writeTimeout(environment.operationTimeout())
            .readTimeout(environment.operationTimeout())
            .build());
    }

    static void writeRecords(PmsClient client, List<TestRecord> records) {
        for (int from = 0; from < records.size(); from += BATCH_SIZE) {
            int to = Math.min(records.size(), from + BATCH_SIZE);
            WriteResult result = client.writeBatch(
                records.subList(from, to).stream().map(TestRecord::toRow).toList()
            );
            requireWrite(result, to - from, "writeBatch");
        }
    }

    static void deleteRecords(PmsClient client, List<Long> ids) {
        for (int from = 0; from < ids.size(); from += BATCH_SIZE) {
            int to = Math.min(ids.size(), from + BATCH_SIZE);
            List<GenericRow> rows = new ArrayList<>(to - from);
            for (long id : ids.subList(from, to)) {
                rows.add(GenericRow.ofKind(
                    RowKind.DELETE,
                    id,
                    BinaryString.fromString("ignored-delete-payload"),
                    0
                ));
            }
            requireWrite(client.writeBatch(rows), rows.size(), "deleteBatch");
        }
    }

    static void assertAllLocal(
            PmsClient client,
            Map<Long, TestRecord> expected,
            List<Long> deletedIds) {
        expected.values().forEach(record -> assertHit(client.getLocal(record.keyTuple()), record));
        deletedIds.forEach(id -> assertEquals(
            LookupResultType.DELETED,
            client.getLocal(GenericRow.of(id)).type(),
            "Expected local tombstone for id=" + id
        ));
    }

    static void assertHistoricalSample(
            PmsClient client,
            Map<Long, TestRecord> expected,
            List<Long> deletedIds,
            int limit) {
        expected.values().stream().limit(limit).forEach(record -> {
            assertEquals(
                LookupResultType.MISS,
                client.getLocal(record.keyTuple()).type(),
                "Fresh local state should miss id=" + record.id()
            );
            assertHit(client.get(record.keyTuple()), record);
        });
        deletedIds.stream().limit(limit).forEach(id -> {
            GenericRow key = GenericRow.of(id);
            assertEquals(LookupResultType.MISS, client.getLocal(key).type());
            assertAbsent(client.get(key));
        });
    }

    static void assertAllFull(PmsClient client, Map<Long, TestRecord> expected, List<Long> deletedIds) {
        expected.values().forEach(record -> assertHit(client.get(record.keyTuple()), record));
        deletedIds.forEach(id -> assertAbsent(client.get(GenericRow.of(id))));
    }

    private static void assertAbsent(PmsRowLookupResult result) {
        assertEquals(PmsStatus.OK, result.status());
        // Historical tombstones can be retained or removed by Paimon compaction.
        assertTrue(result.type() == LookupResultType.MISS || result.type() == LookupResultType.DELETED,
            "Deleted key reappeared: " + result);
        assertNull(result.row());
    }

    static long number(Map<String, Object> state, String field) {
        return ((Number) state.get(field)).longValue();
    }

    static void assertHit(PmsRowLookupResult result, TestRecord expected) {
        assertEquals(PmsStatus.OK, result.status(), "lookup status for id=" + expected.id());
        assertEquals(LookupResultType.HIT, result.type(), "lookup type for id=" + expected.id());
        assertNotNull(result.row());
        assertEquals(expected, TestRecord.fromRow(result.row()), "row for id=" + expected.id());
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> nestedMap(Map<String, Object> state, String field) {
        Object value = state.get(field);
        if (!(value instanceof Map<?, ?> map)) {
            throw new AssertionError("Expected state." + field + " to be an object: " + value);
        }
        return (Map<String, Object>) map;
    }

    private static void requireWrite(WriteResult result, int expectedCount, String operation) {
        assertEquals(PmsStatus.OK, result.status(), operation + " status");
        assertEquals(expectedCount, result.acceptedCount(), operation + " acceptedCount");
    }
}
