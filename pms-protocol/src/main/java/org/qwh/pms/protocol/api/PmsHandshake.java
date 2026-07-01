package org.qwh.pms.protocol.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PmsHandshake(
        String protocol,
        int protocolVersion,
        String requiredHttpVersion,
        String backend,
        int maxKeyBytes,
        int maxRowBytes,
        int maxBatchEntries,
        int maxConcurrentStreams,
        int maxRequestBodyBytes,
        int maxResponseBodyBytes,
        List<String> capabilities) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public PmsHandshake {
        Objects.requireNonNull(protocol, "protocol must not be null");
        Objects.requireNonNull(requiredHttpVersion, "requiredHttpVersion must not be null");
        Objects.requireNonNull(backend, "backend must not be null");
        Objects.requireNonNull(capabilities, "capabilities must not be null");
        requirePositive(protocolVersion, "protocolVersion");
        requirePositive(maxKeyBytes, "maxKeyBytes");
        requirePositive(maxRowBytes, "maxRowBytes");
        requirePositive(maxBatchEntries, "maxBatchEntries");
        requirePositive(maxConcurrentStreams, "maxConcurrentStreams");
        requirePositive(maxRequestBodyBytes, "maxRequestBodyBytes");
        requirePositive(maxResponseBodyBytes, "maxResponseBodyBytes");
        capabilities = List.copyOf(capabilities);
    }

    public static PmsHandshake fromJson(String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return MAPPER.readValue(json, PmsHandshake.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new ProtocolNegotiationException("failed to decode PMS handshake", e);
        }
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new ProtocolNegotiationException("failed to encode PMS handshake", e);
        }
    }

    public void requireCompatible() {
        if (!PmsProtocolConstants.PROTOCOL_NAME.equals(protocol)) {
            throw new ProtocolNegotiationException("unsupported PMS protocol: " + protocol);
        }
        if (protocolVersion != PmsProtocolConstants.PROTOCOL_VERSION) {
            throw new ProtocolNegotiationException("unsupported PMS protocol version: " + protocolVersion);
        }
        if (!PmsProtocolConstants.REQUIRED_HTTP_VERSION.equals(requiredHttpVersion)) {
            throw new ProtocolNegotiationException("unsupported required HTTP version: " + requiredHttpVersion);
        }
        requireCapabilities(PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES.toArray(String[]::new));
    }

    public void requireCapabilities(String... requiredCapabilities) {
        Objects.requireNonNull(requiredCapabilities, "requiredCapabilities must not be null");
        for (String capability : requiredCapabilities) {
            if (!capabilities.contains(capability)) {
                throw new ProtocolNegotiationException("server does not support capability: " + capability);
            }
        }
    }

    public boolean supports(String capability) {
        return capabilities.contains(Objects.requireNonNull(capability, "capability must not be null"));
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new ProtocolNegotiationException(name + " must be positive: " + value);
        }
    }
}
