package org.qwh.pms.flink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PmsFlinkTypeAdapterTest {

    @Test
    void acceptsMatchingServerSchemaWithoutDependingOnPaimonFieldIds() {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        org.apache.paimon.types.RowType serverType =
                org.apache.paimon.types.DataTypes.ROW(
                                org.apache.paimon.types.DataTypes.FIELD(
                                        101,
                                        "id",
                                        org.apache.paimon.types.DataTypes.INT().notNull()),
                                org.apache.paimon.types.DataTypes.FIELD(
                                        202,
                                        "marker",
                                        org.apache.paimon.types.DataTypes.STRING()));

        assertDoesNotThrow(
                () ->
                        PmsFlinkTypeAdapter.validateServerSchema(
                                schema, serverType, List.of("id")));
    }

    @Test
    void rejectsSchemaOrPrimaryKeyMismatch() {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        org.apache.paimon.types.RowType renamed =
                org.apache.paimon.types.DataTypes.ROW(
                                org.apache.paimon.types.DataTypes.FIELD(
                                        1,
                                        "other_id",
                                        org.apache.paimon.types.DataTypes.INT().notNull()),
                                org.apache.paimon.types.DataTypes.FIELD(
                                        2,
                                        "marker",
                                        org.apache.paimon.types.DataTypes.STRING()))
                        .notNull();

        assertThrows(
                ValidationException.class,
                () ->
                        PmsFlinkTypeAdapter.validateServerSchema(
                                schema, renamed, List.of("id")));
        assertThrows(
                ValidationException.class,
                () ->
                        PmsFlinkTypeAdapter.validateServerSchema(
                                schema,
                                PmsFlinkTestUtils.simplePaimonRowType(),
                                List.of("marker")));
    }

    @Test
    void enforcesCurrentPrimaryKeyTypeProfile() {
        RowType booleanKey =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "enabled",
                                                DataTypes.BOOLEAN().notNull()),
                                        DataTypes.FIELD("marker", DataTypes.STRING()))
                                .notNull()
                                .getLogicalType();
        assertThrows(
                ValidationException.class,
                () ->
                        PmsFlinkTestUtils.tableSchema(
                                booleanKey, List.of("enabled")));

        RowType timestampMicros =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "ts",
                                                DataTypes.TIMESTAMP(6).notNull()),
                                        DataTypes.FIELD("marker", DataTypes.STRING()))
                                .notNull()
                                .getLogicalType();
        assertDoesNotThrow(
                () ->
                        PmsFlinkTestUtils.tableSchema(
                                timestampMicros, List.of("ts")));

        RowType timestampNanos =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "ts",
                                                DataTypes.TIMESTAMP(9).notNull()),
                                        DataTypes.FIELD("marker", DataTypes.STRING()))
                                .notNull()
                                .getLogicalType();
        assertThrows(
                ValidationException.class,
                () ->
                        PmsFlinkTestUtils.tableSchema(
                                timestampNanos, List.of("ts")));
    }
}
