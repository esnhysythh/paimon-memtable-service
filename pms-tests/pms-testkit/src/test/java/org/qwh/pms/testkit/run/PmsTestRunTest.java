package org.qwh.pms.testkit.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.testkit.environment.CleanupPolicy;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PmsTestRunTest {

    @Test
    void ownershipReadPreservesLineBreaks(@TempDir Path tempDir) throws Exception {
        String marker = "pms-it-owner-v1\nrunId=example\n";
        var path = new org.apache.paimon.fs.Path(tempDir.resolve("owner").toUri());
        try (var fileIO = org.apache.paimon.fs.local.LocalFileIO.create()) {
            fileIO.writeFile(path, marker, false);
            assertEquals(marker, PmsTestRun.readExactUtf8(fileIO, path));
        }
    }

    @Test
    void alwaysPolicyDeletesSuccessfulOwnedLocalRun(@TempDir Path tempDir) throws Exception {
        PmsTestEnvironment environment = environment(tempDir, CleanupPolicy.ALWAYS);
        Path runPath;
        try (PmsTestRun run = PmsTestRun.create(environment, "unit")) {
            runPath = run.localRunRoot();
            assertTrue(Files.isRegularFile(runPath.resolve(PmsTestRun.OWNER_MARKER)));
            run.markSuccessful();
        }

        assertFalse(Files.exists(runPath));
        var report = new ObjectMapper().readTree(environment.localRoot().resolve("reports")
            .resolve(runPath.getFileName() + ".json").toFile());
        assertTrue(report.get("successful").asBoolean());
        assertTrue(report.get("cleanupCompleted").asBoolean());
        assertEquals("0.1-SNAPSHOT", report.get("projectVersion").asText());
        assertEquals("1.4.1", report.get("paimonVersion").asText());
        assertTrue(report.has("gitCommit"));
        assertTrue(report.has("gitDirty"));
    }

    @Test
    void teardownFailureClosesRemainingResourcesAndRetainsScene(@TempDir Path tempDir) throws Exception {
        PmsTestEnvironment environment = environment(tempDir, CleanupPolicy.ON_SUCCESS);
        PmsTestRun run = PmsTestRun.create(environment, "unit");
        List<Integer> closed = new ArrayList<>();
        run.own(() -> closed.add(1));
        run.own(() -> { closed.add(2); throw new IOException("shutdown failed"); });
        run.markSuccessful();
        assertEquals("shutdown failed", assertThrows(IOException.class, run::close).getMessage());
        assertEquals(List.of(2, 1), closed);
        assertTrue(Files.exists(run.localRunRoot()));
        var report = new ObjectMapper().readTree(environment.localRoot().resolve("reports")
            .resolve(run.runId() + ".json").toFile());
        assertFalse(report.get("successful").asBoolean());
        assertFalse(report.get("cleanupCompleted").asBoolean());
    }

    @Test
    void onSuccessPolicyPreservesFailedRun(@TempDir Path tempDir) throws Exception {
        PmsTestEnvironment environment = environment(tempDir, CleanupPolicy.ON_SUCCESS);
        Path runPath;
        try (PmsTestRun run = PmsTestRun.create(environment, "unit")) {
            runPath = run.localRunRoot();
        }

        assertTrue(Files.isRegularFile(runPath.resolve(PmsTestRun.OWNER_MARKER)));
        assertTrue(Files.isRegularFile(runPath.resolve("run-manifest.json")));
    }

    @Test
    void refusesCleanupWhenOwnerMarkerWasChanged(@TempDir Path tempDir) throws Exception {
        PmsTestEnvironment environment = environment(tempDir, CleanupPolicy.ALWAYS);
        PmsTestRun run = PmsTestRun.create(environment, "unit");
        Path runPath = run.localRunRoot();
        Files.writeString(runPath.resolve(PmsTestRun.OWNER_MARKER), "not-this-run\n");
        run.markSuccessful();

        assertThrows(IOException.class, run::close);
        assertTrue(Files.exists(runPath));
    }

    private static PmsTestEnvironment environment(Path tempDir, CleanupPolicy cleanup) throws Exception {
        Path javaHome = tempDir.resolve("jdk");
        Path java = javaHome.resolve("bin/java");
        Files.createDirectories(java.getParent());
        Files.createFile(java);
        java.toFile().setExecutable(true);

        Path hadoopConf = tempDir.resolve("hadoop-conf");
        Files.createDirectories(hadoopConf);
        Files.writeString(hadoopConf.resolve("core-site.xml"), "<configuration/>");
        Files.writeString(hadoopConf.resolve("hdfs-site.xml"), "<configuration/>");
        return new PmsTestEnvironment(
            javaHome.toAbsolutePath(),
            hadoopConf.toAbsolutePath(),
            tempDir.resolve("runs").toAbsolutePath(),
            URI.create("hdfs://namenode/tmp/pms-it-unit"),
            "test-classpath",
            cleanup,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1)
        );
    }
}
