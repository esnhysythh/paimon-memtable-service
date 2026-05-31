package org.qwh.pms.core.sink;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

public final class SinkMetaStore {
    private static final int VERSION = 1;
    private static final String PREPARE_SUFFIX = ".prepare.json";
    private static final String SUCCESS_SUFFIX = ".success.json";
    private static final String ENCODED_PREPARE_FIELD = "encodedPrepareBase64";
    private static final String ENCODED_SUCCESS_FIELD = "encodedSuccessBase64";

    private final Path dir;

    public SinkMetaStore(Path dir) {
        if (dir == null) {
            throw new NullPointerException("dir must not be null");
        }
        this.dir = dir;
    }

    public void init() throws IOException {
        Files.createDirectories(dir);
    }

    public void savePrepare(PreparedSinkCommit prepared) {
        byte[] encoded = SinkMetaPayloadCodec.encodePrepare(prepared);
        String body = prepareJson(prepared, encoded);
        writeAtomically(preparePath(prepared.batchId()), withChecksum(body));
    }

    public void saveSuccess(SinkCommitResult result) {
        byte[] encoded = SinkMetaPayloadCodec.encodeSuccess(result);
        String body = successJson(result, encoded);
        writeAtomically(successPath(result.batchId()), withChecksum(body));
    }

    public SinkRecoveryState load() {
        Map<String, PreparedSinkCommit> prepares = new HashMap<>();
        Set<String> successes = new HashSet<>();
        Set<Long> sinkedSSTIds = new HashSet<>();
        long lastSnapshotId = 0;
        long lastPersistedSequenceId = 0;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + PREPARE_SUFFIX)) {
            for (Path path : stream) {
                PreparedSinkCommit prepared = readPrepare(path);
                prepares.put(prepared.batchId(), prepared);
            }
        } catch (IOException e) {
            throw new RuntimeException("load sink prepare metadata failed: " + dir, e);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + SUCCESS_SUFFIX)) {
            for (Path path : stream) {
                SinkCommitResult success = readSuccess(path);
                successes.add(success.batchId());
                sinkedSSTIds.addAll(success.sstIds());
                lastSnapshotId = Math.max(lastSnapshotId, success.snapshotId());
                lastPersistedSequenceId = Math.max(lastPersistedSequenceId, success.persistedSequenceId());
            }
        } catch (IOException e) {
            throw new RuntimeException("load sink success metadata failed: " + dir, e);
        }

        for (String batchId : successes) {
            prepares.remove(batchId);
        }
        return new SinkRecoveryState(
            sinkedSSTIds,
            prepares.values().stream()
                .sorted(
                    Comparator.comparingLong(PreparedSinkCommit::minSequenceId)
                        .thenComparingLong(PreparedSinkCommit::maxSequenceId)
                        .thenComparing(PreparedSinkCommit::batchId)
                )
                .toList(),
            lastSnapshotId,
            lastPersistedSequenceId
        );
    }

    private PreparedSinkCommit readPrepare(Path path) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        verifyChecksum(path, content);
        requireVersion(content, "sink prepare metadata");
        String encoded = readStringField(content, ENCODED_PREPARE_FIELD);
        PreparedSinkCommit prepared = SinkMetaPayloadCodec.decodePrepare(Base64.getDecoder().decode(encoded));
        validatePrepareMetadata(path, content, prepared);
        return prepared;
    }

    private SinkCommitResult readSuccess(Path path) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        verifyChecksum(path, content);
        requireVersion(content, "sink success metadata");
        String encoded = readStringField(content, ENCODED_SUCCESS_FIELD);
        SinkCommitResult result = SinkMetaPayloadCodec.decodeSuccess(Base64.getDecoder().decode(encoded));
        validateSuccessMetadata(path, content, result);
        return result;
    }

    private Path preparePath(String batchId) {
        return dir.resolve(fileSafeBatchId(batchId) + PREPARE_SUFFIX);
    }

    private Path successPath(String batchId) {
        return dir.resolve(fileSafeBatchId(batchId) + SUCCESS_SUFFIX);
    }

    private static String prepareJson(PreparedSinkCommit prepared, byte[] encoded) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        field(out, "version", VERSION, true);
        field(out, "batchId", prepared.batchId(), true);
        field(out, "commitIdentifier", prepared.commitIdentifier(), true);
        longArrayField(out, "sstIds", prepared.sstIds(), true);
        field(out, "minSequenceId", prepared.minSequenceId(), true);
        field(out, "maxSequenceId", prepared.maxSequenceId(), true);
        field(out, "paimonCommitPayloadBase64", Base64.getEncoder().encodeToString(prepared.payload()), true);
        out.append("  \"fileRefs\": [\n");
        for (int i = 0; i < prepared.fileRefs().size(); i++) {
            SinkFileRef ref = prepared.fileRefs().get(i);
            out.append("    {\n");
            field(out, "fileName", ref.fileName(), true, 6);
            field(out, "path", ref.path(), true, 6);
            field(out, "fileSize", ref.fileSize(), true, 6);
            field(out, "rowCount", ref.rowCount(), true, 6);
            field(out, "partition", ref.partition(), true, 6);
            field(out, "bucket", ref.bucket(), false, 6);
            out.append("    }");
            if (i + 1 < prepared.fileRefs().size()) {
                out.append(",");
            }
            out.append("\n");
        }
        out.append("  ],\n");
        field(out, "inputRecordCount", prepared.inputRecordCount(), true);
        field(out, "outputRecordCount", prepared.outputRecordCount(), true);
        field(out, ENCODED_PREPARE_FIELD, Base64.getEncoder().encodeToString(encoded), false);
        out.append("}\n");
        return out.toString();
    }

    private static String successJson(SinkCommitResult result, byte[] encoded) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        field(out, "version", VERSION, true);
        field(out, "batchId", result.batchId(), true);
        field(out, "snapshotId", result.snapshotId(), true);
        field(out, "persistedSequenceId", result.persistedSequenceId(), true);
        longArrayField(out, "sstIds", result.sstIds(), true);
        field(out, ENCODED_SUCCESS_FIELD, Base64.getEncoder().encodeToString(encoded), false);
        out.append("}\n");
        return out.toString();
    }

    private static String withChecksum(String body) {
        String prefix = body.substring(0, body.lastIndexOf("}\n"));
        StringBuilder out = new StringBuilder(prefix);
        if (!prefix.endsWith("{\n")) {
            out.append(",\n");
        }
        out.append("  \"metaCrc32\": ").append(checksum(body)).append("\n");
        out.append("}\n");
        return out.toString();
    }

    private static void verifyChecksum(Path path, String content) {
        long expected = readLongField(content, "metaCrc32");
        String body = content.replaceFirst(",?\\n\\s*\"metaCrc32\"\\s*:\\s*\\d+\\s*\\n}\\s*$", "}\n");
        long actual = checksum(body);
        if (expected != actual) {
            throw new IllegalArgumentException("Corrupt sink metadata checksum: " + path);
        }
    }

    private void writeAtomically(Path target, String content) {
        try {
            Files.createDirectories(dir);
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            Files.writeString(
                tmp,
                content,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
            try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(dir);
        } catch (IOException e) {
            throw new RuntimeException("write sink metadata failed: " + target, e);
        }
    }

    private static void field(StringBuilder out, String name, String value, boolean comma) {
        field(out, name, value, comma, 2);
    }

    private static void field(StringBuilder out, String name, String value, boolean comma, int indent) {
        indent(out, indent).append('"').append(name).append("\": \"").append(escape(value)).append('"');
        appendCommaAndNewline(out, comma);
    }

    private static void field(StringBuilder out, String name, long value, boolean comma) {
        field(out, name, value, comma, 2);
    }

    private static void field(StringBuilder out, String name, long value, boolean comma, int indent) {
        indent(out, indent).append('"').append(name).append("\": ").append(value);
        appendCommaAndNewline(out, comma);
    }

    private static void longArrayField(StringBuilder out, String name, Iterable<Long> values, boolean comma) {
        indent(out, 2).append('"').append(name).append("\": [");
        boolean first = true;
        for (long value : values) {
            if (!first) {
                out.append(", ");
            }
            out.append(value);
            first = false;
        }
        out.append("]");
        appendCommaAndNewline(out, comma);
    }

    private static StringBuilder indent(StringBuilder out, int spaces) {
        return out.append(" ".repeat(spaces));
    }

    private static void appendCommaAndNewline(StringBuilder out, boolean comma) {
        if (comma) {
            out.append(",");
        }
        out.append("\n");
    }

    private static String readStringField(String content, String name) {
        String marker = "\"" + name + "\"";
        int field = content.indexOf(marker);
        if (field < 0) {
            throw new IllegalArgumentException("Missing field: " + name);
        }
        int colon = content.indexOf(':', field + marker.length());
        int start = content.indexOf('"', colon + 1);
        if (colon < 0 || start < 0) {
            throw new IllegalArgumentException("Invalid string field: " + name);
        }
        StringBuilder value = new StringBuilder();
        boolean escaping = false;
        for (int i = start + 1; i < content.length(); i++) {
            char c = content.charAt(i);
            if (escaping) {
                value.append(switch (c) {
                    case '"', '\\', '/' -> c;
                    case 'n' -> '\n';
                    case 'r' -> '\r';
                    case 't' -> '\t';
                    default -> c;
                });
                escaping = false;
            } else if (c == '\\') {
                escaping = true;
            } else if (c == '"') {
                return value.toString();
            } else {
                value.append(c);
            }
        }
        throw new IllegalArgumentException("Unterminated string field: " + name);
    }

    private static long readLongField(String content, String name) {
        String marker = "\"" + name + "\"";
        int field = content.indexOf(marker);
        if (field < 0) {
            throw new IllegalArgumentException("Missing field: " + name);
        }
        int colon = content.indexOf(':', field + marker.length());
        if (colon < 0) {
            throw new IllegalArgumentException("Invalid numeric field: " + name);
        }
        int start = colon + 1;
        while (start < content.length() && Character.isWhitespace(content.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < content.length() && Character.isDigit(content.charAt(end))) {
            end++;
        }
        if (end == start) {
            throw new IllegalArgumentException("Invalid numeric field: " + name);
        }
        return Long.parseLong(content.substring(start, end));
    }

    private static String escape(String value) {
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private static String fileSafeBatchId(String batchId) {
        if (batchId.matches("[A-Za-z0-9._-]+")) {
            return batchId;
        }
        return "b64-" + Base64.getUrlEncoder().withoutPadding().encodeToString(batchId.getBytes(StandardCharsets.UTF_8));
    }

    private static void requireVersion(String content, String label) {
        long version = readLongField(content, "version");
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported " + label + " version: " + version);
        }
    }

    private static void validatePrepareMetadata(Path path, String content, PreparedSinkCommit prepared) {
        if (!prepared.batchId().equals(readStringField(content, "batchId"))
            || prepared.commitIdentifier() != readLongField(content, "commitIdentifier")
            || prepared.minSequenceId() != readLongField(content, "minSequenceId")
            || prepared.maxSequenceId() != readLongField(content, "maxSequenceId")
            || prepared.inputRecordCount() != readLongField(content, "inputRecordCount")
            || prepared.outputRecordCount() != readLongField(content, "outputRecordCount")) {
            throw new IllegalArgumentException("Sink prepare metadata does not match payload: " + path);
        }
    }

    private static void validateSuccessMetadata(Path path, String content, SinkCommitResult result) {
        if (!result.batchId().equals(readStringField(content, "batchId"))
            || result.snapshotId() != readLongField(content, "snapshotId")
            || result.persistedSequenceId() != readLongField(content, "persistedSequenceId")) {
            throw new IllegalArgumentException("Sink success metadata does not match payload: " + path);
        }
    }

    private static long checksum(String content) {
        CRC32 crc32 = new CRC32();
        crc32.update(content.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }

    private static void forceDirectory(Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
