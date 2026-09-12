package org.qwh.pms.testkit.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.qwh.pms.testkit.data.TestRecord;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PaimonVerifier {
    private PaimonVerifier() {}

    public static Map<Long, TestRecord> readCurrentRows(
            PmsTestEnvironment environment,
            URI warehouse,
            PaimonTestTableSpec spec) throws Exception {
        try (Catalog catalog = PaimonCatalogs.open(environment, warehouse)) {
            Table table = catalog.getTable(spec.identifier());
            ReadBuilder readBuilder = table.newReadBuilder();
            Map<Long, TestRecord> records = new LinkedHashMap<>();
            try (RecordReader<InternalRow> reader =
                     readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
                reader.forEachRemaining(row -> {
                    TestRecord record = TestRecord.fromRow(row);
                    TestRecord previous = records.put(record.id(), record);
                    if (previous != null) {
                        throw new IllegalStateException("Duplicate primary key in Paimon scan: " + record.id());
                    }
                });
            }
            return Map.copyOf(records);
        }
    }

    public static long latestSnapshotId(
            PmsTestEnvironment environment,
            URI warehouse,
            PaimonTestTableSpec spec) throws Exception {
        try (Catalog catalog = PaimonCatalogs.open(environment, warehouse)) {
            return catalog.getTable(spec.identifier()).latestSnapshot()
                .map(snapshot -> snapshot.id())
                .orElse(0L);
        }
    }
}
