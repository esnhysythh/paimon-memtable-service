package org.qwh.pms.testkit.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;
import org.qwh.pms.testkit.paimon.PaimonCatalogs;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class PmsTestRun implements AutoCloseable {
    public static final String OWNER_MARKER = ".pms-it-owner";
    private static final String OWNER_MARKER_HEADER = "pms-it-owner-v1";

    private static final DateTimeFormatter RUN_TIME = DateTimeFormatter
        .ofPattern("yyyyMMddHHmmss")
        .withZone(ZoneOffset.UTC);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    private final PmsTestEnvironment environment;
    private final String scenario;
    private final String runId;
    private final java.nio.file.Path localRunRoot;
    private final URI hdfsRunRoot;
    private final String createdAt;
    private final String ownerMarkerContents;
    private final Deque<AutoCloseable> resources = new ArrayDeque<>();

    private boolean remoteReady;
    private boolean successful;
    private boolean closed;

    private PmsTestRun(PmsTestEnvironment environment, String scenario, String runId) throws IOException {
        this.environment = Objects.requireNonNull(environment, "environment must not be null");
        this.scenario = requireScenario(scenario);
        this.runId = runId;
        this.localRunRoot = environment.localRoot().resolve(runId).normalize();
        this.hdfsRunRoot = append(environment.hdfsRoot(), runId);
        this.createdAt = Instant.now().toString();
        this.ownerMarkerContents = OWNER_MARKER_HEADER + "\nrunId=" + runId + "\n";
        createLocalResources();
    }

    public static PmsTestRun create(PmsTestEnvironment environment, String scenario) throws IOException {
        String runId = RUN_TIME.format(Instant.now()) + "-" + UUID.randomUUID().toString().substring(0, 8);
        return new PmsTestRun(environment, scenario, runId);
    }

    public String runId() {
        return runId;
    }

    public java.nio.file.Path localRunRoot() {
        return localRunRoot;
    }

    public URI warehouseUri() {
        return append(hdfsRunRoot, "warehouse");
    }

    public java.nio.file.Path phaseRoot(String phase) {
        return localRunRoot.resolve("phases").resolve(requireScenario(phase));
    }

    public java.nio.file.Path logPath(String phase) {
        return localRunRoot.resolve("logs").resolve(requireScenario(phase) + ".out");
    }

    public synchronized void prepareRemote() throws IOException {
        requireOpen();
        if (remoteReady) {
            return;
        }
        try (FileIO fileIO = PaimonCatalogs.fileIO(environment, hdfsRunRoot)) {
            Path root = new Path(hdfsRunRoot);
            if (fileIO.exists(root)) {
                throw new IOException("Refusing to take over existing HDFS run directory: " + hdfsRunRoot);
            }
            if (!fileIO.mkdirs(root)) {
                throw new IOException("Unable to create HDFS run directory: " + hdfsRunRoot);
            }
            fileIO.writeFile(new Path(root, OWNER_MARKER), ownerMarkerContents, false);
            remoteReady = true;
        }
    }

    public void verifyRemoteRoundTrip() throws IOException {
        requireRemoteReady();
        try (FileIO fileIO = PaimonCatalogs.fileIO(environment, hdfsRunRoot)) {
            Path probe = new Path(hdfsRunRoot.toString(), "preflight-write.txt");
            Path renamed = new Path(hdfsRunRoot.toString(), "preflight-renamed.txt");
            String contents = "pms-it-preflight:\n" + runId + "\n";
            fileIO.writeFile(probe, contents, false);
            if (!contents.equals(readExactUtf8(fileIO, probe))) {
                throw new IOException("HDFS preflight read did not match written content: " + probe);
            }
            if (!fileIO.rename(probe, renamed) || !fileIO.exists(renamed)) {
                throw new IOException("HDFS preflight rename failed: " + probe + " -> " + renamed);
            }
            if (!fileIO.delete(renamed, false) || fileIO.exists(renamed)) {
                throw new IOException("HDFS preflight delete failed: " + renamed);
            }
        }
    }

    public synchronized <T extends AutoCloseable> T own(T resource) {
        requireOpen();
        resources.push(Objects.requireNonNull(resource, "resource must not be null"));
        return resource;
    }

    public synchronized void markSuccessful() {
        requireOpen();
        successful = true;
    }

    public synchronized void recordMetadata(Map<String, ?> metadata) throws IOException {
        requireOpen();
        Objects.requireNonNull(metadata, "metadata must not be null");
        java.nio.file.Path manifestPath = localRunRoot.resolve("run-manifest.json");
        @SuppressWarnings("unchecked")
        Map<String, Object> manifest = OBJECT_MAPPER.readValue(manifestPath.toFile(), Map.class);
        metadata.forEach((key, value) -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("manifest metadata key must not be blank");
            }
            manifest.put(key, value);
        });
        Files.writeString(
            manifestPath,
            OBJECT_MAPPER.writeValueAsString(manifest),
            StandardCharsets.UTF_8
        );
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        Exception failure = null;
        while (!resources.isEmpty()) {
            try {
                resources.pop().close();
            } catch (Exception e) {
                failure = addSuppressed(failure, e);
            }
        }
        // Retain a small report outside the disposable run directory, including on success.
        java.nio.file.Path reportPath = environment.localRoot().resolve("reports").resolve(runId + ".json");
        Map<String, Object> report = new LinkedHashMap<>();
        try {
            report.putAll(OBJECT_MAPPER.readValue(localRunRoot.resolve("run-manifest.json").toFile(), Map.class));
            report.put("finishedAt", Instant.now().toString());
            report.put("successful", successful && failure == null);
            report.put("cleanupCompleted", false);
            if (failure != null) {
                report.put("teardownFailure", failure.toString());
            }
            Files.createDirectories(reportPath.getParent());
            OBJECT_MAPPER.writeValue(reportPath.toFile(), report);
        } catch (Exception e) {
            failure = addSuppressed(failure, e);
        }
        if (failure == null && environment.cleanupPolicy().shouldCleanup(successful)) {
            try {
                cleanupRemote();
            } catch (Exception e) {
                failure = addSuppressed(failure, e);
            }
            if (failure == null) {
                try {
                    cleanupLocal();
                    report.put("cleanupCompleted", true);
                } catch (Exception e) {
                    failure = addSuppressed(failure, e);
                }
            }
        }
        try {
            if (failure != null) {
                report.put("successful", false);
                report.put("teardownFailure", failure.toString());
            }
            OBJECT_MAPPER.writeValue(reportPath.toFile(), report);
        } catch (Exception e) {
            failure = addSuppressed(failure, e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void createLocalResources() throws IOException {
        if (!localRunRoot.getParent().equals(environment.localRoot())) {
            throw new IOException("Unsafe local run path: " + localRunRoot);
        }
        Files.createDirectories(environment.localRoot());
        try {
            Files.createDirectory(localRunRoot);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            throw new IOException("Refusing to take over existing local run directory: " + localRunRoot, e);
        }
        Files.createDirectories(localRunRoot.resolve("conf"));
        Files.createDirectories(localRunRoot.resolve("logs"));
        Files.createDirectories(localRunRoot.resolve("run"));
        Files.createDirectories(localRunRoot.resolve("phases"));
        Files.writeString(
            localRunRoot.resolve(OWNER_MARKER),
            ownerMarkerContents,
            StandardCharsets.UTF_8
        );

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("formatVersion", 1);
        manifest.put("runId", runId);
        manifest.put("scenario", scenario);
        manifest.put("createdAt", createdAt);
        manifest.put("localRunRoot", localRunRoot.toString());
        manifest.put("hdfsRunRoot", hdfsRunRoot.toString());
        manifest.put("warehouse", warehouseUri().toString());
        manifest.put("cleanupPolicy", environment.cleanupPolicy().propertyValue());
        manifest.put("javaHome", environment.pmsJavaHome().toString());
        manifest.put("testJvm", System.getProperty("java.runtime.version"));
        manifest.put("hadoopConfDir", environment.hadoopConfDir().toString());
        Properties build = new Properties();
        try (var input = PmsTestRun.class.getResourceAsStream("/pms-testkit-build.properties")) {
            if (input == null) {
                throw new IOException("Missing pms-testkit-build.properties; rebuild testkit with Maven");
            }
            build.load(input);
        }
        build.forEach((key, value) -> manifest.put(key.toString(), value));
        manifest.put("gitCommit", git("rev-parse", "HEAD"));
        String gitStatus = git("status", "--porcelain");
        manifest.put("gitDirty", "unknown".equals(gitStatus) ? "unknown" : !gitStatus.isBlank());
        Files.writeString(
            localRunRoot.resolve("run-manifest.json"),
            OBJECT_MAPPER.writeValueAsString(manifest),
            StandardCharsets.UTF_8
        );
    }

    private void cleanupRemote() throws IOException {
        if (!remoteReady) {
            return;
        }
        URI expected = append(environment.hdfsRoot(), runId);
        if (!expected.equals(hdfsRunRoot)) {
            throw new IOException("Unsafe HDFS cleanup target: " + hdfsRunRoot);
        }
        try (FileIO fileIO = PaimonCatalogs.fileIO(environment, hdfsRunRoot)) {
            Path root = new Path(hdfsRunRoot);
            Path owner = new Path(root, OWNER_MARKER);
            if (!fileIO.exists(owner)) {
                throw new IOException("HDFS owner marker is missing: " + owner);
            }
            requireMatchingMarker(readExactUtf8(fileIO, owner), "HDFS " + owner);
            if (!fileIO.delete(root, true) && fileIO.exists(root)) {
                throw new IOException("Unable to delete owned HDFS run directory: " + root);
            }
        }
    }

    private void cleanupLocal() throws IOException {
        if (!localRunRoot.getParent().equals(environment.localRoot())
                || !localRunRoot.getFileName().toString().equals(runId)) {
            throw new IOException("Unsafe local cleanup target: " + localRunRoot);
        }
        java.nio.file.Path owner = localRunRoot.resolve(OWNER_MARKER);
        if (!Files.isRegularFile(owner)) {
            throw new IOException("Local owner marker is missing: " + owner);
        }
        requireMatchingMarker(Files.readString(owner, StandardCharsets.UTF_8), "local " + owner);
        try (var paths = Files.walk(localRunRoot)) {
            for (java.nio.file.Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    private void requireMatchingMarker(String contents, String location) throws IOException {
        if (!ownerMarkerContents.equals(contents)) {
            throw new IOException("Owner marker mismatch at " + location);
        }
    }

    static String readExactUtf8(FileIO fileIO, Path path) throws IOException {
        // FileIO.readFileUtf8 concatenates lines, which changes our ownership marker.
        try (var input = fileIO.newInputStream(path)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void requireRemoteReady() {
        requireOpen();
        if (!remoteReady) {
            throw new IllegalStateException("Remote resources are not prepared for run " + runId);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Run is already closed: " + runId);
        }
    }

    private static Exception addSuppressed(Exception current, Exception additional) {
        if (current == null) {
            return additional;
        }
        current.addSuppressed(additional);
        return current;
    }

    private static URI append(URI root, String child) {
        String value = root.toString();
        return URI.create((value.endsWith("/") ? value : value + "/") + child);
    }

    private static String requireScenario(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9-]{0,47}")) {
            throw new IllegalArgumentException("scenario/phase must match [a-z][a-z0-9-]{0,47}: " + value);
        }
        return value;
    }

    private static String git(String... arguments) {
        var command = new java.util.ArrayList<String>();
        command.add("git");
        command.addAll(java.util.List.of(arguments));
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start();
            // Drain stdout before waiting so a large dirty worktree cannot fill the pipe.
            Process child = process;
            var output = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
                } catch (IOException e) {
                    return "unknown";
                }
            });
            if (process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0) {
                return output.get(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // Source metadata is unavailable outside a Git checkout.
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
        }
        return "unknown";
    }
}
