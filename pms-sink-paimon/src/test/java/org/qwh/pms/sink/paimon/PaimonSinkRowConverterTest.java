package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSinkRowConverterTest {

    private final RowType rowType = DataTypes.ROW(
        DataTypes.FIELD(1, "id", DataTypes.INT()),
        DataTypes.FIELD(2, "marker", DataTypes.STRING())
    );
    private final PmsRowValueCodec rowValueCodec = new PmsRowValueCodec();
    private final PmsPrimaryKeyCodec primaryKeyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id"));
    private final PaimonSinkRowConverter converter = new PaimonSinkRowConverter(rowType, List.of("id"));

    @Test
    void convertsRowValueToInsertRow() {
        GenericRow row = GenericRow.of(7, BinaryString.fromString("seven"));
        Entry entry = new Entry(key(row), new Value(rowValueCodec.encode(rowType, row, 0), 1));

        InternalRow converted = converter.toPaimonRow(entry);

        assertEquals(RowKind.INSERT, converted.getRowKind());
        assertEquals(7, converted.getInt(0));
        assertEquals("seven", converted.getString(1).toString());
    }

    @Test
    void convertsTombstoneToDeleteRowWithPrimaryKeyOnly() {
        GenericRow row = GenericRow.of(7, BinaryString.fromString("seven"));
        Entry entry = new Entry(key(row), Value.tombstone(2));

        InternalRow converted = converter.toPaimonRow(entry);

        assertEquals(RowKind.DELETE, converted.getRowKind());
        assertEquals(7, converted.getInt(0));
        assertTrue(converted.isNullAt(1));
    }

    private Key key(GenericRow row) {
        return new Key(primaryKeyCodec.encodeKey(row));
    }
}
