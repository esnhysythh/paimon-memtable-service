package org.qwh.pms.core.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkMetaStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void loadReturnsPendingPrepareWhenSuccessIsMissing() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");

        store.savePrepare(prepared);

        SinkRecoveryState state = store.load();

        assertEquals(1, state.pendingPrepares().size());
        assertEquals("sink-10-2", state.pendingPrepares().get(0).batchId());
        assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), state.pendingPrepares().get(0).payload());
        assertTrue(state.sinkedSSTIds().isEmpty());
    }

    @Test
    void loadReturnsPendingPreparesInSequenceOrder() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit later = prepared("sink-20-2", 20L, 29L);
        PreparedSinkCommit earlier = prepared("sink-10-2", 10L, 19L);

        store.savePrepare(later);
        store.savePrepare(earlier);

        SinkRecoveryState state = store.load();

        assertEquals(List.of("sink-10-2", "sink-20-2"), state.pendingPrepares().stream()
            .map(PreparedSinkCommit::batchId)
            .toList());
    }

    @Test
    void loadTreatsSuccessSstIdsAsSinkedAndRemovesPendingPrepare() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");

        store.savePrepare(prepared);
        store.saveSuccess(new SinkCommitResult(
            prepared.batchId(),
            42L,
            prepared.maxSequenceId(),
            prepared.sstIds(),
            prepared.payload()
        ));

        SinkRecoveryState state = store.load();

        assertTrue(state.pendingPrepares().isEmpty());
        assertEquals(42L, state.lastSinkedSnapshotId());
        assertEquals(prepared.maxSequenceId(), state.lastPersistedSequenceId());
        assertEquals(prepared.sstIds().size(), state.sinkedSSTIds().size());
        assertTrue(state.sinkedSSTIds().containsAll(prepared.sstIds()));
    }

    @Test
    void loadsExactDurableSuccessByBatchId() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");
        SinkCommitResult success = new SinkCommitResult(
            prepared.batchId(),
            42L,
            prepared.maxSequenceId(),
            prepared.sstIds(),
            new byte[] {7, 8, 9}
        );
        store.savePrepare(prepared);
        store.saveSuccess(success);

        SinkCommitResult loaded = store.loadSuccess(prepared.batchId()).orElseThrow();

        assertEquals(success.batchId(), loaded.batchId());
        assertEquals(success.snapshotId(), loaded.snapshotId());
        assertEquals(success.persistedSequenceId(), loaded.persistedSequenceId());
        assertEquals(success.sstIds(), loaded.sstIds());
        assertArrayEquals(success.commitPayload(), loaded.commitPayload());
        assertTrue(store.loadSuccess("missing-batch").isEmpty());
    }

    @Test
    void metadataIsHumanReadableJson() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");

        store.savePrepare(prepared);

        String content = Files.readString(tempDir.resolve("sink-10-2.prepare.json"), StandardCharsets.UTF_8);
        assertTrue(content.contains("\"batchId\": \"sink-10-2\""));
        assertTrue(content.contains("\"sstIds\": [1, 2]"));
        assertTrue(content.contains("\"paimonCommitPayloadBase64\""));
        assertTrue(content.contains("\"fileRefs\""));
    }

    @Test
    void corruptMetadataChecksumIsRejected() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");
        store.savePrepare(prepared);

        Path path = tempDir.resolve("sink-10-2.prepare.json");
        String content = Files.readString(path, StandardCharsets.UTF_8)
            .replace("\"batchId\": \"sink-10-2\"", "\"batchId\": \"sink-10-3\"");
        Files.writeString(path, content, StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, store::load);
    }

    @Test
    void unsupportedMetadataVersionIsRejected() throws IOException {
        SinkMetaStore store = store();
        PreparedSinkCommit prepared = prepared("sink-10-2");
        store.savePrepare(prepared);

        Path path = tempDir.resolve("sink-10-2.prepare.json");
        String content = Files.readString(path, StandardCharsets.UTF_8)
            .replace("\"version\": 1", "\"version\": 2");
        long checksum = checksum(content.replaceFirst(",?\\n\\s*\"metaCrc32\"\\s*:\\s*\\d+\\s*\\n}\\s*$", "}\n"));
        content = content.replaceFirst("\"metaCrc32\"\\s*:\\s*\\d+", "\"metaCrc32\": " + checksum);
        Files.writeString(path, content, StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, store::load);
    }

    private SinkMetaStore store() throws IOException {
        SinkMetaStore store = new SinkMetaStore(tempDir);
        store.init();
        return store;
    }

    private static PreparedSinkCommit prepared(String batchId) {
        return prepared(batchId, 1L, 10L);
    }

    private static PreparedSinkCommit prepared(String batchId, long minSequenceId, long maxSequenceId) {
        return new PreparedSinkCommit(
            batchId,
            10L,
            List.of(1L, 2L),
            minSequenceId,
            maxSequenceId,
            "payload".getBytes(StandardCharsets.UTF_8),
            List.of(new SinkFileRef("file.orc", "/tmp/file.orc", 12L, 3L, "{}", 0)),
            4L,
            3L
        );
    }

    private static long checksum(String content) {
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(content.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }
}
