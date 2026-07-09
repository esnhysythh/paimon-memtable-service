package org.qwh.pms.client;

import org.qwh.pms.protocol.api.PmsHandshake;

import java.time.Duration;

interface PmsTransport extends AutoCloseable {

    PmsHandshake handshake();

    PmsHttpResponse postBinary(String path, byte[] body, Duration timeout, int maxResponseBodyBytes);

    @Override
    default void close() {}
}
