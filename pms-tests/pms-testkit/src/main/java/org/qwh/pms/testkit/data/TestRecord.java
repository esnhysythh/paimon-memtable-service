package org.qwh.pms.testkit.data;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;

import java.util.Objects;

public record TestRecord(long id, String payload, int version) {

    public TestRecord {
        Objects.requireNonNull(payload, "payload must not be null");
    }

    public GenericRow toRow() {
        return GenericRow.ofKind(
            version == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER,
            id,
            BinaryString.fromString(payload),
            version
        );
    }

    public GenericRow keyTuple() {
        return GenericRow.of(id);
    }

    public static TestRecord fromRow(InternalRow row) {
        Objects.requireNonNull(row, "row must not be null");
        if (row.getFieldCount() != 3) {
            throw new IllegalArgumentException("Expected 3 fields, found " + row.getFieldCount());
        }
        return new TestRecord(row.getLong(0), row.getString(1).toString(), row.getInt(2));
    }
}
