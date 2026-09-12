package org.qwh.pms.testkit.environment;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsTestEnvironmentTest {

    @Test
    void cleanupPoliciesAreExplicit() {
        assertTrue(CleanupPolicy.ALWAYS.shouldCleanup(false));
        assertTrue(CleanupPolicy.ON_SUCCESS.shouldCleanup(true));
        assertFalse(CleanupPolicy.ON_SUCCESS.shouldCleanup(false));
        assertFalse(CleanupPolicy.NEVER.shouldCleanup(true));
        assertEquals(CleanupPolicy.ON_SUCCESS, CleanupPolicy.parse("on-success"));
        assertThrows(IllegalArgumentException.class, () -> CleanupPolicy.parse("sometimes"));
    }

    @Test
    void parsesHumanFriendlyAndIsoDurations() {
        assertEquals(Duration.ofMillis(250), PmsTestEnvironment.parseDuration("250ms", "test"));
        assertEquals(Duration.ofSeconds(30), PmsTestEnvironment.parseDuration("30s", "test"));
        assertEquals(Duration.ofMinutes(2), PmsTestEnvironment.parseDuration("2m", "test"));
        assertEquals(Duration.ofSeconds(5), PmsTestEnvironment.parseDuration("PT5S", "test"));
    }

    @Test
    void rejectsBroadHdfsRootsBeforeExternalAccess() {
        Path javaHome = Path.of("/nonexistent/java-home");
        Path conf = Path.of("/nonexistent/hadoop-conf");
        Path local = Path.of("/tmp/pms-it-tests");

        assertThrows(IllegalArgumentException.class, () -> new PmsTestEnvironment(
            javaHome,
            conf,
            local,
            URI.create("hdfs://namenode/tmp"),
            "classpath",
            CleanupPolicy.NEVER,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1)
        ));
        assertThrows(IllegalArgumentException.class, () -> new PmsTestEnvironment(
            javaHome,
            conf,
            local,
            URI.create("file:///tmp/pms-it"),
            "classpath",
            CleanupPolicy.NEVER,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1)
        ));
        assertThrows(IllegalArgumentException.class, () -> new PmsTestEnvironment(
            javaHome,
            conf,
            local,
            URI.create("hdfs://namenode/tmp/pms-it/../shared"),
            "classpath",
            CleanupPolicy.NEVER,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1)
        ));
    }
}
