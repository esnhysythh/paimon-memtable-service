package org.qwh.pms.server;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/** Keeps Paimon commit identifiers scoped to one lifetime of PMS local state. */
final class PmsWriterIdentity {
    private PmsWriterIdentity() {}

    static String loadOrCreate(Path walDir, Path storageDir, String prefix) throws IOException {
        if (prefix.contains("\n") || prefix.contains("\r")) {
            throw new IllegalArgumentException("pms.server.commit_user must be a single line");
        }
        Path identityFile = storageDir.resolve("commit-user");
        Path temporaryFile = storageDir.resolve("commit-user.tmp");
        String commitUser;
        if (Files.exists(identityFile)) {
            commitUser = Files.readString(identityFile).strip();
            if (!commitUser.matches(Pattern.quote(prefix) + "-[0-9a-f]{12}")) {
                throw new IOException(
                    "Invalid writer identity or changed pms.server.commit_user: " + identityFile
                );
            }
        } else {
            // A missing identity is only safe before any WAL/SST/Sink metadata has been created.
            // A temporary identity can survive interrupted initialization, before data is accepted.
            for (Path dir : List.of(walDir, storageDir)) {
                if (Files.exists(dir)) {
                    try (var entries = Files.newDirectoryStream(dir)) {
                        for (Path entry : entries) {
                            if (!entry.equals(temporaryFile)) {
                                throw new IOException(
                                    "Missing writer identity " + identityFile
                                        + " for existing local state " + entry
                                        + "; restore the original identity with its state"
                                );
                            }
                        }
                    }
                }
            }
            // 48 random bits keep the name short for infrequent local-state replacements.
            commitUser = prefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            Files.createDirectories(storageDir);
            Files.writeString(
                temporaryFile,
                commitUser + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.SYNC
            );
            Files.move(temporaryFile, identityFile, StandardCopyOption.ATOMIC_MOVE);
        }
        // Publish the identity durably before opening WAL or recovering a prepared Paimon commit.
        try (FileChannel directory = FileChannel.open(storageDir, StandardOpenOption.READ)) {
            directory.force(true);
        }
        return commitUser;
    }
}
