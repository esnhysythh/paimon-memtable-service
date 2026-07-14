package org.qwh.pms.sink.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void convertsTombstoneToDeleteRowWithSyntheticNotNullFields() {
        RowType notNullRowType = DataTypes.ROW(
            DataTypes.FIELD(1, "id", DataTypes.INT()),
            DataTypes.FIELD(2, "marker", DataTypes.STRING().notNull())
        );
        PaimonSinkRowConverter notNullConverter =
            new PaimonSinkRowConverter(notNullRowType, List.of("id"));
        PmsPrimaryKeyCodec keyCodec =
            PmsPrimaryKeyCodec.forFieldNames(notNullRowType, List.of("id"));
        GenericRow row = GenericRow.of(7, BinaryString.fromString("seven"));
        Entry entry = new Entry(new Key(keyCodec.encodeKey(row)), Value.tombstone(2));

        InternalRow converted = notNullConverter.toPaimonRow(entry);

        assertEquals(RowKind.DELETE, converted.getRowKind());
        assertEquals(7, converted.getInt(0));
        assertEquals("", converted.getString(1).toString());
    }

    @Test
    void convertsTombstoneToDeleteRowWithSyntheticComplexFields() {
        RowType nestedType = DataTypes.ROW(
            DataTypes.FIELD(31, "required", DataTypes.STRING().notNull()),
            DataTypes.FIELD(32, "optional", DataTypes.INT())
        ).notNull();
        RowType complexRowType = DataTypes.ROW(
            DataTypes.FIELD(1, "id", DataTypes.INT()),
            DataTypes.FIELD(2, "items", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
            DataTypes.FIELD(
                3,
                "attributes",
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT().notNull()).notNull()
            ),
            DataTypes.FIELD(4, "counts", DataTypes.MULTISET(DataTypes.STRING().notNull()).notNull()),
            DataTypes.FIELD(5, "details", nestedType),
            DataTypes.FIELD(6, "amount", DataTypes.DECIMAL(38, 18).notNull())
        );
        PaimonSinkRowConverter complexConverter =
            new PaimonSinkRowConverter(complexRowType, List.of("id"));
        PmsPrimaryKeyCodec keyCodec =
            PmsPrimaryKeyCodec.forFieldNames(complexRowType, List.of("id"));
        GenericRow keyRow = new GenericRow(complexRowType.getFieldCount());
        keyRow.setField(0, 7);
        Entry entry = new Entry(new Key(keyCodec.encodeKey(keyRow)), Value.tombstone(2));

        InternalRow converted = complexConverter.toPaimonRow(entry);

        assertEquals(RowKind.DELETE, converted.getRowKind());
        assertEquals(0, converted.getArray(1).size());
        assertEquals(0, converted.getMap(2).size());
        assertEquals(0, converted.getMap(3).size());
        InternalRow details = converted.getRow(4, nestedType.getFieldCount());
        assertEquals("", details.getString(0).toString());
        assertTrue(details.isNullAt(1));
        assertEquals(Decimal.zero(38, 18), converted.getDecimal(5, 38, 18));
    }

    @Test
    void rejectsUnknownPrimaryKeyNameWithClearMessage() {
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> new PaimonSinkRowConverter(rowType, List.of("missing"))
        );

        assertTrue(error.getMessage().contains("Unknown primary key field: missing"));
    }

    private Key key(GenericRow row) {
        return new Key(primaryKeyCodec.encodeKey(row));
    }
}
