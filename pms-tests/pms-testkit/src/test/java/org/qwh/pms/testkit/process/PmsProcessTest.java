package org.qwh.pms.testkit.process;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PmsProcessTest {
    @Test
    void gracefulTimeoutKillsChildAndStillFails() throws Exception {
        Process child = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"), HangingShutdown.class.getName()
        ).redirectErrorStream(true).start();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                assertEquals("ready", new BufferedReader(new InputStreamReader(child.getInputStream())).readLine());
            });
            IOException failure = assertThrows(IOException.class,
                () -> PmsProcess.terminateProcess(child, false, Duration.ofSeconds(1)));
            assertTrue(failure.getMessage().contains("did not stop"));
            assertFalse(child.isAlive(), "A failed shutdown must not leak the child");
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
    }

    public static class HangingShutdown {
        public static void main(String[] args) throws Exception {
            CountDownLatch forever = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    forever.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            System.out.println("ready");
            System.out.flush();
            forever.await();
        }
    }
}
