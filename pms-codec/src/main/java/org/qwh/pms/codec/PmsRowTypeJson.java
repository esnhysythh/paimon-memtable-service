package org.qwh.pms.codec;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Objects;

public final class PmsRowTypeJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PmsRowTypeJson() {}

    public static String serialize(RowType rowType) {
        Objects.requireNonNull(rowType, "rowType must not be null");
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = MAPPER.getFactory().createGenerator(writer)) {
            rowType.serializeJson(generator);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to serialize Paimon RowType to JSON", e);
        }
        return writer.toString();
    }

    public static RowType deserialize(String rowTypeJson) {
        Objects.requireNonNull(rowTypeJson, "rowTypeJson must not be null");
        try {
            JsonNode node = MAPPER.readTree(rowTypeJson);
            DataType dataType = DataTypeJsonParser.parseDataType(node);
            if (dataType instanceof RowType rowType) {
                return rowType;
            }
            throw new IllegalArgumentException("PMS table schema rowTypeJson is not a Paimon RowType: " + dataType);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("failed to parse Paimon RowType JSON", e);
        }
    }
}
