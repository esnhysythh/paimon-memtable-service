package org.qwh.pms.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsConnectorConfigTest {

    @Test
    void defaultsSelectAsyncLookupAndHttp2() {
        Configuration options = new Configuration();
        options.set(PmsConnectorOptions.ENDPOINT, "http://127.0.0.1:9090");

        PmsConnectorConfig config = PmsConnectorConfig.from(options);

        assertTrue(config.lookupAsync());
        assertTrue(config.requireHttp2());
        assertEquals(4, config.lookupAsyncThreadNumber());
        assertEquals(256, config.sinkBatchMaxRows());
    }

    @Test
    void rejectsInvalidEndpointAndRanges() {
        Configuration invalidEndpoint = new Configuration();
        invalidEndpoint.set(PmsConnectorOptions.ENDPOINT, "127.0.0.1:9090");
        assertThrows(
                ValidationException.class,
                () -> PmsConnectorConfig.from(invalidEndpoint));

        Configuration endpointWithCredentials = new Configuration();
        endpointWithCredentials.set(
                PmsConnectorOptions.ENDPOINT,
                "http://user:secret@127.0.0.1:9090/");
        assertThrows(
                ValidationException.class,
                () -> PmsConnectorConfig.from(endpointWithCredentials));

        Configuration invalidBackoff = new Configuration();
        invalidBackoff.set(
                PmsConnectorOptions.ENDPOINT, "http://127.0.0.1:9090");
        invalidBackoff.set(
                PmsConnectorOptions.LOOKUP_RETRY_INITIAL_BACKOFF,
                Duration.ofSeconds(2));
        invalidBackoff.set(
                PmsConnectorOptions.LOOKUP_RETRY_MAX_BACKOFF,
                Duration.ofSeconds(1));
        assertThrows(
                ValidationException.class,
                () -> PmsConnectorConfig.from(invalidBackoff));
    }
}
