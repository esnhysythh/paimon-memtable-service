package org.qwh.pms.protocol.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsHandshakeTest {

    @Test
    void jsonRoundTripPreservesProtocolLimitsAndCapabilities() {
        PmsHandshake handshake = compatibleHandshake();

        PmsHandshake decoded = PmsHandshake.fromJson(handshake.toJson());

        assertEquals(PmsProtocolConstants.PROTOCOL_NAME, decoded.protocol());
        assertEquals(PmsProtocolConstants.PROTOCOL_VERSION, decoded.protocolVersion());
        assertEquals(PmsProtocolConstants.REQUIRED_HTTP_VERSION, decoded.requiredHttpVersion());
        assertEquals("pms", decoded.backend());
        assertEquals(65_536, decoded.maxKeyBytes());
        assertEquals(16_777_216, decoded.maxRowBytes());
        assertEquals(1_024, decoded.maxBatchEntries());
        assertEquals(512, decoded.maxConcurrentStreams());
        assertEquals(33_554_432, decoded.maxRequestBodyBytes());
        assertEquals(33_554_432, decoded.maxResponseBodyBytes());
        assertNull(decoded.tableSchema());
        assertTrue(decoded.supports(PmsProtocolConstants.CAPABILITY_LOCAL_WRITE_BATCH));
    }

    @Test
    void jsonRoundTripPreservesTableSchemaSnapshot() {
        PmsTableSchema tableSchema = PmsTableSchema.create(
            7,
            PmsTableSchema.ROW_TYPE_FORMAT_PAIMON_JSON_V1,
            "{\"type\":\"ROW\",\"fields\":[]}",
            List.of("id"),
            List.of("dt"),
            1,
            1
        );
        PmsHandshake handshake = new PmsHandshake(
            PmsProtocolConstants.PROTOCOL_NAME,
            PmsProtocolConstants.PROTOCOL_VERSION,
            PmsProtocolConstants.REQUIRED_HTTP_VERSION,
            "pms",
            65_536,
            16_777_216,
            1_024,
            512,
            33_554_432,
            33_554_432,
            tableSchema,
            PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES
        );

        PmsHandshake decoded = PmsHandshake.fromJson(handshake.toJson());

        assertNotNull(decoded.tableSchema());
        decoded.tableSchema().requireHashMatches();
        assertEquals(7, decoded.tableSchema().schemaId());
        assertEquals(List.of("id"), decoded.tableSchema().primaryKeys());
        assertEquals(List.of("dt"), decoded.tableSchema().partitionKeys());
        assertEquals(tableSchema.schemaHash(), decoded.tableSchema().schemaHash());
    }

    @Test
    void requireCompatibleAcceptsHotPathCapabilities() {
        compatibleHandshake().requireCompatible();
    }

    @Test
    void requireCompatibleRejectsMissingCapability() {
        PmsHandshake handshake = new PmsHandshake(
                PmsProtocolConstants.PROTOCOL_NAME,
                PmsProtocolConstants.PROTOCOL_VERSION,
                PmsProtocolConstants.REQUIRED_HTTP_VERSION,
                "pms",
                1,
                1,
                1,
                1,
                1,
                1,
                List.of(PmsProtocolConstants.CAPABILITY_LOCAL_PUT));

        assertThrows(ProtocolNegotiationException.class, handshake::requireCompatible);
        assertFalse(handshake.supports(PmsProtocolConstants.CAPABILITY_FULL_GET));
    }

    @Test
    void fromJsonRejectsInvalidLimits() {
        String json = """
                {
                  "protocol": "pms-http2-binary",
                  "protocolVersion": 1,
                  "requiredHttpVersion": "HTTP_2",
                  "backend": "pms",
                  "maxKeyBytes": 0,
                  "maxRowBytes": 1,
                  "maxBatchEntries": 1,
                  "maxConcurrentStreams": 1,
                  "maxRequestBodyBytes": 1,
                  "maxResponseBodyBytes": 1,
                  "capabilities": []
                }
                """;

        assertThrows(ProtocolNegotiationException.class, () -> PmsHandshake.fromJson(json));
    }

    private static PmsHandshake compatibleHandshake() {
        return new PmsHandshake(
                PmsProtocolConstants.PROTOCOL_NAME,
                PmsProtocolConstants.PROTOCOL_VERSION,
                PmsProtocolConstants.REQUIRED_HTTP_VERSION,
                "pms",
                65_536,
                16_777_216,
                1_024,
                512,
                33_554_432,
                33_554_432,
                List.of(
                        PmsProtocolConstants.CAPABILITY_LOCAL_PUT,
                        PmsProtocolConstants.CAPABILITY_LOCAL_DELETE,
                        PmsProtocolConstants.CAPABILITY_LOCAL_WRITE_BATCH,
                        PmsProtocolConstants.CAPABILITY_LOCAL_GET,
                        PmsProtocolConstants.CAPABILITY_LOCAL_GET_PREFIX,
                        PmsProtocolConstants.CAPABILITY_FULL_GET,
                        PmsProtocolConstants.CAPABILITY_STRICT_HTTP2));
    }
}
