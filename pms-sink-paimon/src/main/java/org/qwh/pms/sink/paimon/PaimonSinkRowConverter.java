package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.memtable.model.Entry;

import java.util.List;

public final class PaimonSinkRowConverter {
    private final RowType rowType;
    private final PmsRowValueCodec rowValueCodec;
    private final DeleteRowFactory deleteRowFactory;

    public PaimonSinkRowConverter(RowType rowType, List<String> primaryKeyNames) {
        this(rowType, primaryKeyNames, new PmsRowValueCodec());
    }

    public PaimonSinkRowConverter(
            RowType rowType, List<String> primaryKeyNames, PmsRowValueCodec rowValueCodec) {
        if (rowType == null) {
            throw new NullPointerException("rowType must not be null");
        }
        if (primaryKeyNames == null || primaryKeyNames.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyNames must not be empty");
        }
        this.rowType = rowType;
        this.rowValueCodec = rowValueCodec;
        this.deleteRowFactory = new DeleteRowFactory(rowType, primaryKeyNames);
    }

    public InternalRow toPaimonRow(Entry entry) {
        if (entry.value().isTombstone()) {
            return deleteRow(entry);
        }
        InternalRow row = rowValueCodec.decode(rowType, entry.value().bytes());
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private InternalRow deleteRow(Entry entry) {
        return deleteRowFactory.createDeleteRow(entry.key().bytes());
    }
}
