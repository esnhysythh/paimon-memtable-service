package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

final class SSTMetaStore {
    private static final int VERSION = 2;
    private static final String SUFFIX = ".meta.json";

    private final Path dir;

    SSTMetaStore(Path dir) {
        if (dir == null) {
            throw new NullPointerException("dir must not be null");
        }
        this.dir = dir;
    }

    void init() throws IOException {
        Files.createDirectories(dir);
    }

    List<Path> listMetaFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sst-*" + SUFFIX)) {
            for (Path path : stream) {
                files.add(path);
            }
        }
        files.sort(Comparator.comparingLong((Path path) -> flushRangeFromMetaPath(path)[0])
            .thenComparingLong(path -> flushRangeFromMetaPath(path)[1]));
        return files;
    }

    void save(SSTMeta meta) throws IOException {
        String body = toJson(meta);
        writeAtomically(metaPath(meta), withChecksum(body));
    }

    SSTMeta load(Path path) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        verifyChecksum(path, content);
        requireVersion(content);
        long runId = readLongField(content, "runId");
        long minFlushId = readLongField(content, "minFlushId");
        long maxFlushId = readLongField(content, "maxFlushId");
        Path sstPath = existingSstPath(minFlushId, maxFlushId, readStringField(content, "sstFile"));
        return new SSTMeta(
            runId,
            minFlushId,
            maxFlushId,
            sstPath,
            readLongField(content, "fileSize"),
            readLongField(content, "entryCount"),
            keyField(content, "minKeyBase64"),
            keyField(content, "maxKeyBase64"),
            readLongField(content, "minSequenceId"),
            readLongField(content, "maxSequenceId"),
            readLongField(content, "oldestWriteAtMillis"),
            readLongField(content, "createdAtMillis"),
            SSTState.valueOf(readStringField(content, "state"))
        );
    }

    Path metaPath(SSTMeta meta) {
        return metaPath(meta.minFlushId(), meta.maxFlushId());
    }

    Path metaPath(long minFlushId, long maxFlushId) {
        return dir.resolve(String.format("sst-%06d-%06d%s", minFlushId, maxFlushId, SUFFIX));
    }

    static long[] flushRangeFromMetaPath(Path path) {
        String name = path.getFileName().toString();
        if (!name.startsWith("sst-") || !name.endsWith(SUFFIX)) {
            return new long[] {0, 0};
        }
        String range = name.substring(4, name.length() - SUFFIX.length());
        String[] parts = range.split("-");
        if (parts.length >= 2) {
            return new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        }
        long id = Long.parseLong(range);
        return new long[] {id, id};
    }

    private Path existingSstPath(long minFlushId, long maxFlushId, String recordedFile) {
        Path recordedName = Path.of(recordedFile).getFileName();
        Path recorded = dir.resolve(recordedName == null ? recordedFile : recordedName.toString());
        if (Files.exists(recorded)) {
            return recorded;
        }
        Path rangePath = SSTWriter.pathFor(dir, minFlushId, maxFlushId, SSTState.NEW);
        if (Files.exists(rangePath)) {
            return rangePath;
        }
        return recorded;
    }

    private static String toJson(SSTMeta meta) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        field(out, "version", VERSION, true);
        field(out, "runId", meta.runId(), true);
        field(out, "minFlushId", meta.minFlushId(), true);
        field(out, "maxFlushId", meta.maxFlushId(), true);
        field(out, "sstFile", meta.path().getFileName().toString(), true);
        field(out, "state", meta.state().name(), true);
        field(out, "fileSize", meta.fileSize(), true);
        field(out, "entryCount", meta.entryCount(), true);
        field(out, "minKeyBase64", encodeKey(meta.minKey()), true);
        field(out, "maxKeyBase64", encodeKey(meta.maxKey()), true);
        field(out, "minSequenceId", meta.minSequenceId(), true);
        field(out, "maxSequenceId", meta.maxSequenceId(), true);
        field(out, "oldestWriteAtMillis", meta.oldestWriteAtMillis(), true);
        field(out, "createdAtMillis", meta.createdAtMillis(), false);
        out.append("}\n");
        return out.toString();
    }

    private static String encodeKey(Key key) {
        if (key == null) {
            return "";
        }
        return Base64.getEncoder().encodeToString(key.bytes());
    }

    private static Key keyField(String content, String name) {
        String encoded = readStringField(content, name);
        if (encoded.isEmpty()) {
            return null;
        }
        return new Key(Base64.getDecoder().decode(encoded));
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
            throw new IllegalArgumentException("Corrupt SST metadata checksum: " + path);
        }
    }

    private static void requireVersion(String content) {
        long version = readLongField(content, "version");
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported SST metadata version: " + version);
        }
    }

    private void writeAtomically(Path target, String content) throws IOException {
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
        StorageFiles.forceDirectory(dir);
    }

    private static void field(StringBuilder out, String name, String value, boolean comma) {
        out.append("  \"").append(name).append("\": \"").append(escape(value)).append('"');
        appendCommaAndNewline(out, comma);
    }

    private static void field(StringBuilder out, String name, long value, boolean comma) {
        out.append("  \"").append(name).append("\": ").append(value);
        appendCommaAndNewline(out, comma);
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

    private static long checksum(String content) {
        CRC32 crc32 = new CRC32();
        crc32.update(content.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }
}
