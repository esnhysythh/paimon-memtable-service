package org.qwh.pms.flink.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PmsPrimaryKeySelectorTest {

    @Test
    void extractsPrimaryKeyInDeclaredOrderAndReturnsStableCopies() {
        PmsFlinkTableSchema schema = compositeSchema();
        PmsPrimaryKeySelector selector =
                new PmsPrimaryKeySelector(
                        schema.rowType(),
                        schema.primaryKeyIndexes(),
                        schema.primaryKeyRowType());

        RowData first =
                selector.getKey(
                        GenericRowData.of(
                                StringData.fromString("tenant-a"),
                                StringData.fromString("value-a"),
                                11L));
        RowData second =
                selector.getKey(
                        GenericRowData.of(
                                StringData.fromString("tenant-b"),
                                StringData.fromString("value-b"),
                                22L));

        assertNotSame(first, second);
        assertEquals(11L, first.getLong(0));
        assertEquals("tenant-a", first.getString(1).toString());
        assertEquals(22L, second.getLong(0));
        assertEquals("tenant-b", second.getString(1).toString());
    }

    @Test
    void rejectsNullPrimaryKeyAtRuntime() {
        PmsFlinkTableSchema schema = compositeSchema();
        PmsPrimaryKeySelector selector =
                new PmsPrimaryKeySelector(
                        schema.rowType(),
                        schema.primaryKeyIndexes(),
                        schema.primaryKeyRowType());

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        selector.getKey(
                                GenericRowData.of(
                                        StringData.fromString("tenant-a"),
                                        StringData.fromString("value-a"),
                                        null)));
    }

    private static PmsFlinkTableSchema compositeSchema() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "tenant",
                                                DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("marker", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "user_id",
                                                DataTypes.BIGINT().notNull()))
                                .notNull()
                                .getLogicalType();
        return PmsFlinkTestUtils.tableSchema(
                rowType, List.of("user_id", "tenant"));
    }
}
