package org.qwh.pms.client.examples;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.client.PmsClientConfig;
import org.qwh.pms.client.PmsRowLookupResult;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.WriteResult;

import java.net.URI;
import java.util.Optional;

public final class PmsClientUsageExample {

    private PmsClientUsageExample() {}

    public static Optional<InternalRow> writeReadAndDeleteMarkerRow(
            URI serverUri, int id, String marker) {
        PmsClientConfig config = PmsClientConfig.builder(serverUri).build();
        try (PmsClient client = PmsClient.connect(config)) {
            GenericRow row = new GenericRow(RowKind.INSERT, client.rowType().getFieldCount());
            row.setField(0, id);
            row.setField(1, BinaryString.fromString(marker));

            requireOk(client.write(row), "write");

            GenericRow keyTuple = GenericRow.of(id);
            PmsRowLookupResult lookup = client.get(keyTuple);
            Optional<InternalRow> latest = lookup.rowOptional();

            requireOk(client.delete(keyTuple), "delete");
            return latest;
        }
    }

    private static void requireOk(WriteResult result, String operation) {
        if (result.status() != PmsStatus.OK) {
            throw new IllegalStateException("PMS " + operation + " failed: " + result.status());
        }
    }
}
