package org.qwh.pms.protocol.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PmsTableSchema(
        long schemaId,
        String schemaHash,
        String rowTypeFormat,
        String rowTypeJson,
        List<String> primaryKeys,
        List<String> partitionKeys,
        int rowValueCodecVersion,
        int primaryKeyCodecVersion) {

    public static final String ROW_TYPE_FORMAT_PAIMON_JSON_V1 = "paimon-row-type-json-v1";
    private static final String HASH_PREFIX = "sha256:";

    public PmsTableSchema {
        if (schemaId < 0) {
            throw new ProtocolNegotiationException("schemaId must not be negative: " + schemaId);
        }
        requireNotBlank(schemaHash, "schemaHash");
        requireNotBlank(rowTypeFormat, "rowTypeFormat");
        requireNotBlank(rowTypeJson, "rowTypeJson");
        Objects.requireNonNull(primaryKeys, "primaryKeys must not be null");
        Objects.requireNonNull(partitionKeys, "partitionKeys must not be null");
        requirePositive(rowValueCodecVersion, "rowValueCodecVersion");
        requirePositive(primaryKeyCodecVersion, "primaryKeyCodecVersion");
        primaryKeys = List.copyOf(primaryKeys);
        partitionKeys = List.copyOf(partitionKeys);
        requireListItems(primaryKeys, "primaryKeys");
        requireListItems(partitionKeys, "partitionKeys");
    }

    public static PmsTableSchema create(
            long schemaId,
            String rowTypeFormat,
            String rowTypeJson,
            List<String> primaryKeys,
            List<String> partitionKeys,
            int rowValueCodecVersion,
            int primaryKeyCodecVersion) {
        String schemaHash = computeHash(
            schemaId,
            rowTypeFormat,
            rowTypeJson,
            primaryKeys,
            partitionKeys,
            rowValueCodecVersion,
            primaryKeyCodecVersion
        );
        return new PmsTableSchema(
            schemaId,
            schemaHash,
            rowTypeFormat,
            rowTypeJson,
            primaryKeys,
            partitionKeys,
            rowValueCodecVersion,
            primaryKeyCodecVersion
        );
    }

    public void requireHashMatches() {
        String expected = computeHash(
            schemaId,
            rowTypeFormat,
            rowTypeJson,
            primaryKeys,
            partitionKeys,
            rowValueCodecVersion,
            primaryKeyCodecVersion
        );
        if (!expected.equals(schemaHash)) {
            throw new ProtocolNegotiationException(
                "table schema hash mismatch: expected=" + expected + ", actual=" + schemaHash);
        }
    }

    public static String computeHash(
            long schemaId,
            String rowTypeFormat,
            String rowTypeJson,
            List<String> primaryKeys,
            List<String> partitionKeys,
            int rowValueCodecVersion,
            int primaryKeyCodecVersion) {
        MessageDigest digest = sha256();
        updateLong(digest, schemaId);
        updateString(digest, rowTypeFormat);
        updateString(digest, rowTypeJson);
        updateStringList(digest, primaryKeys);
        updateStringList(digest, partitionKeys);
        updateInt(digest, rowValueCodecVersion);
        updateInt(digest, primaryKeyCodecVersion);
        return HASH_PREFIX + HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    private static void updateStringList(MessageDigest digest, List<String> values) {
        Objects.requireNonNull(values, "values must not be null");
        updateInt(digest, values.size());
        for (String value : values) {
            updateString(digest, value);
        }
    }

    private static void updateString(MessageDigest digest, String value) {
        requireNotBlank(value, "hash field");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new ProtocolNegotiationException(name + " must be positive: " + value);
        }
    }

    private static void requireListItems(List<String> values, String name) {
        for (String value : values) {
            requireNotBlank(value, name + " item");
        }
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new ProtocolNegotiationException(name + " must not be blank");
        }
    }
}
