package org.qwh.pms.lookup.cache;

import org.apache.paimon.io.DataFileMeta;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Owns a dedicated local-cache directory and clears cache files left by a previous process.
 *
 * <p>The directory must not contain user-managed files. Local cache entries are process-local and
 * are not safe to reuse after a restart without a persisted manifest and validation step.
 */
public final class LocalCacheDirectory implements AutoCloseable {

    private final Path root;
    private final Path ownershipLockPath;
    private final FileChannel ownershipLockChannel;
    private final FileLock ownershipLock;
    private boolean closed;

    public LocalCacheDirectory(Path root) throws IOException {
        this.root = root.toAbsolutePath();
        Files.createDirectories(this.root);
        this.ownershipLockPath = this.root.resolve(".paimon-local-lookup.lock");
        this.ownershipLockChannel =
                FileChannel.open(
                        ownershipLockPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE);
        try {
            this.ownershipLock = ownershipLockChannel.tryLock();
            if (ownershipLock == null) {
                throw new IOException("Local cache directory is already owned: " + this.root);
            }
            cleanupOrphans();
        } catch (OverlappingFileLockException e) {
            closeQuietly();
            throw new IOException("Local cache directory is already owned: " + this.root, e);
        } catch (IOException | RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    public File cacheFile(DataFileMeta file, LocalCacheBuildContext context) {
        String identity =
                file.fileName()
                        + '\n'
                        + file.fileSize()
                        + '\n'
                        + file.schemaId()
                        + '\n'
                        + Base64.getUrlEncoder().withoutPadding().encodeToString(context.partition().toBytes())
                        + '\n'
                        + context.bucket();
        return root.resolve(sha256(identity) + ".value-sst").toFile();
    }

    /** Deletes all entries and unfinished build files left in this dedicated cache directory. */
    public void cleanupOrphans() throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(root))
                    .filter(path -> !path.equals(ownershipLockPath))
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    throw new LocalCacheCleanupException(e);
                                }
                            });
        } catch (LocalCacheCleanupException e) {
            throw e.ioCause();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            ownershipLock.release();
        } catch (IOException e) {
            failure = e;
        }
        try {
            ownershipLockChannel.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeQuietly() {
        try {
            ownershipLockChannel.close();
        } catch (IOException ignored) {
            // Constructor failure should retain the original ownership error.
        }
    }

    private static String sha256(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static final class LocalCacheCleanupException extends RuntimeException {

        private LocalCacheCleanupException(IOException cause) {
            super(cause);
        }

        private IOException ioCause() {
            return (IOException) super.getCause();
        }
    }
}
