package org.qwh.pms.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsWriterIdentityTest {
    @TempDir
    Path tempDir;

    @Test
    void identitySurvivesMovingTheLocalState() throws Exception {
        Path storage = tempDir.resolve("storage");
        Path wal = tempDir.resolve("wal");
        Files.createDirectories(wal);
        Files.createDirectories(storage);
        String identity = PmsWriterIdentity.loadOrCreate(wal, storage, "custom-writer");
        assertTrue(identity.matches("custom-writer-[0-9a-f]{12}"));
        Files.writeString(wal.resolve("wal-000001.log"), "existing WAL");

        Path movedStorage = tempDir.resolve("moved-storage");
        Path movedWal = tempDir.resolve("moved-wal");
        Files.move(storage, movedStorage);
        Files.move(wal, movedWal);
        assertEquals(identity, PmsWriterIdentity.loadOrCreate(movedWal, movedStorage, "custom-writer"));
    }

    @Test
    void missingIdentityDoesNotAdoptExistingWalOrStorage() throws Exception {
        for (String directory : List.of("wal", "storage")) {
            Path root = tempDir.resolve(directory);
            Path existing = Files.createDirectories(root.resolve(directory)).resolve("existing-state");
            Files.writeString(existing, "must be preserved");
            IOException error = assertThrows(IOException.class, () -> PmsWriterIdentity.loadOrCreate(
                root.resolve("wal"), root.resolve("storage"), "pms-server"
            ));
            assertTrue(error.getMessage().contains("Missing writer identity"));
            assertFalse(Files.exists(root.resolve("storage/commit-user")));
            assertEquals("must be preserved", Files.readString(existing));
        }
    }

    @Test
    void invalidIdentityIsNotRegenerated() throws Exception {
        Path storage = Files.createDirectories(tempDir.resolve("storage"));
        Path identity = storage.resolve("commit-user");
        for (String invalid : List.of("", "pms-server-123", "invalid")) {
            Files.writeString(identity, invalid);
            assertThrows(IOException.class, () -> PmsWriterIdentity.loadOrCreate(
                tempDir.resolve("wal"), storage, "pms-server"
            ));
            assertEquals(invalid, Files.readString(identity));
        }
    }

    @Test
    void changingPrefixCannotChangeAnExistingWriterIdentity() throws Exception {
        Path wal = tempDir.resolve("wal");
        Path storage = tempDir.resolve("storage");
        String original = PmsWriterIdentity.loadOrCreate(wal, storage, "pms-server");
        assertThrows(IOException.class, () -> PmsWriterIdentity.loadOrCreate(wal, storage, "renamed"));
        assertEquals(original, PmsWriterIdentity.loadOrCreate(wal, storage, "pms-server"));
    }

    @Test
    void interruptedIdentityCreationCanRetryBeforeAnyDataExists() throws Exception {
        Path storage = Files.createDirectories(tempDir.resolve("storage"));
        Files.writeString(storage.resolve("commit-user.tmp"), "partial");
        String identity = PmsWriterIdentity.loadOrCreate(tempDir.resolve("wal"), storage, "pms-server");
        assertTrue(identity.matches("pms-server-[0-9a-f]{12}"));
        assertEquals(identity, Files.readString(storage.resolve("commit-user")).strip());
        assertFalse(Files.exists(storage.resolve("commit-user.tmp")));
    }
}
