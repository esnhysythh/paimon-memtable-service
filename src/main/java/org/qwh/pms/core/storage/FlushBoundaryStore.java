package org.qwh.pms.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

final class FlushBoundaryStore {
    private static final Logger LOG = LoggerFactory.getLogger(FlushBoundaryStore.class);

    private static final String FILE_NAME = "flush-boundary.meta";
    private static final int VERSION = 1;

    private final Path file;

    FlushBoundaryStore(Path dir) {
        this.file = dir.resolve(FILE_NAME);
    }

    long load() throws IOException {
        if (!Files.exists(file)) {
            return 0;
        }

        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            long version = readLong(lines, "version");
            long sequenceId = readLong(lines, "lastFlushedSequenceId");
            long expectedChecksum = readLong(lines, "crc32");
            if (version != VERSION || sequenceId < 0) {
                LOG.warn("Ignore invalid flush boundary metadata: {}", file);
                return 0;
            }

            long actualChecksum = checksum(payload(sequenceId));
            if (actualChecksum != expectedChecksum) {
                LOG.warn("Ignore corrupt flush boundary metadata: {}", file);
                return 0;
            }
            return sequenceId;
        } catch (IllegalArgumentException e) {
            LOG.warn("Ignore unreadable flush boundary metadata: {}", file, e);
            return 0;
        }
    }

    void save(long sequenceId) throws IOException {
        if (sequenceId < 0) {
            throw new IllegalArgumentException("sequenceId must be non-negative");
        }

        Files.createDirectories(file.getParent());
        String payload = payload(sequenceId);
        String content = payload + "crc32=" + checksum(payload) + "\n";
        Path tmp = file.resolveSibling(FILE_NAME + ".tmp");
        Files.writeString(
            tmp,
            content,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        );

        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String payload(long sequenceId) {
        return "version=" + VERSION + "\n"
            + "lastFlushedSequenceId=" + sequenceId + "\n";
    }

    private static long checksum(String payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }

    private static long readLong(List<String> lines, String key) {
        String prefix = key + "=";
        for (String line : lines) {
            if (line.startsWith(prefix)) {
                return Long.parseLong(line.substring(prefix.length()));
            }
        }
        throw new IllegalArgumentException("Missing field: " + key);
    }
}
