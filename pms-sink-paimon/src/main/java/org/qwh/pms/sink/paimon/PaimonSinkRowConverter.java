package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.memtable.model.Entry;

import java.util.List;

public final class PaimonSinkRowConverter {
    private final RowType rowType;
    private final PmsRowValueCodec rowValueCodec;
    private final PmsPrimaryKeyCodec primaryKeyCodec;
    private final DataField[] primaryKeyFields;
    private final int[] primaryKeyOrdinals;
    private final InternalRow.FieldGetter[] primaryKeyGetters;

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
        this.primaryKeyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeyNames);
        this.primaryKeyFields = new DataField[primaryKeyNames.size()];
        this.primaryKeyOrdinals = new int[primaryKeyNames.size()];
        this.primaryKeyGetters = new InternalRow.FieldGetter[primaryKeyNames.size()];
        for (int i = 0; i < primaryKeyNames.size(); i++) {
            DataField field = rowType.getField(primaryKeyNames.get(i));
            primaryKeyFields[i] = field;
            primaryKeyOrdinals[i] = rowType.getFieldIndexByFieldId(field.id());
            primaryKeyGetters[i] = InternalRow.createFieldGetter(field.type(), i);
        }
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
        InternalRow keyTuple = primaryKeyCodec.decodeKey(entry.key().bytes());
        GenericRow row = new GenericRow(RowKind.DELETE, rowType.getFieldCount());
        for (int i = 0; i < primaryKeyFields.length; i++) {
            row.setField(primaryKeyOrdinals[i], primaryKeyGetters[i].getFieldOrNull(keyTuple));
        }
        return row;
    }
}
