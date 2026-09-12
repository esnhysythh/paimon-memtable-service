package org.qwh.pms.testkit.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsAdminClientTest {

    @Test
    void parsesFenceAndCompletionBoundary() throws Exception {
        PmsAdminClient.OperationTicket ticket = PmsAdminClient.parseOperationTicket(
            "sink",
            Map.of(
                "operation", "SINK",
                "fenceSequenceId", 42,
                "completionBoundary", "lastPersistedSequenceId"
            )
        );

        assertEquals("SINK", ticket.operation());
        assertEquals(42, ticket.fenceSequenceId());
        assertFalse(PmsAdminClient.isComplete(Map.of("lastPersistedSequenceId", 41), ticket));
        assertTrue(PmsAdminClient.isComplete(Map.of("lastPersistedSequenceId", 42), ticket));
    }

    @Test
    void rejectsMalformedAdminResponse() {
        assertThrows(
            IOException.class,
            () -> PmsAdminClient.parseOperationTicket(
                "flush",
                Map.of("operation", "FLUSH", "fenceSequenceId", "not-a-number")
            )
        );
        assertThrows(
            IOException.class,
            () -> PmsAdminClient.parseOperationTicket(
                "flush",
                Map.of(
                    "operation", "SINK",
                    "fenceSequenceId", 1,
                    "completionBoundary", "lastPersistedSequenceId"
                )
            )
        );
    }
}
