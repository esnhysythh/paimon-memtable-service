package org.qwh.pms.testkit.process;

import org.qwh.pms.client.PmsClient;
import org.qwh.pms.client.PmsClientConfig;
import org.qwh.pms.testkit.environment.PmsTestEnvironment;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class PmsProcess implements AutoCloseable {
    private static final String MAIN_CLASS = "org.qwh.pms.server.PmsServerMain";

    private final PmsTestEnvironment environment;
    private final PmsProcessConfig config;
    private final String runId;
    private Process process;
    private boolean stopped;

    private PmsProcess(
            PmsTestEnvironment environment,
            PmsProcessConfig config,
            String runId) {
        this.environment = Objects.requireNonNull(environment, "environment must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.runId = Objects.requireNonNull(runId, "runId must not be null");
    }

    public static PmsProcess start(
            PmsTestEnvironment environment,
            PmsProcessConfig config,
            String runId) throws IOException {
        PmsProcess result = new PmsProcess(environment, config, runId);
        result.startProcess();
        return result;
    }

    public URI baseUri() {
        return config.baseUri();
    }

    public synchronized void stop() throws IOException {
        terminate(false);
    }

    public synchronized void forceKill() throws IOException {
        terminate(true);
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    private List<String> command() {
        List<String> command = new ArrayList<>();
        command.add(environment.javaExecutable().toString());
        command.add("-Djava.io.tmpdir=" + config.phaseRoot().resolve("tmp"));
        command.add("-Dpms.log.level=INFO");
        command.add("-cp");
        // Hadoop Configuration loads XML resources from the classpath; HADOOP_CONF_DIR
        // alone is interpreted by Hadoop shell scripts, not a directly launched JVM.
        command.add(environment.hadoopConfDir() + java.io.File.pathSeparator + environment.serverClasspath());
        command.add(MAIN_CLASS);
        command.add(config.propertiesPath().toString());
        return List.copyOf(command);
    }

    private void startProcess() throws IOException {
        config.writeProperties(runId);
        ProcessBuilder builder = new ProcessBuilder(command());
        builder.environment().put("JAVA_HOME", environment.pmsJavaHome().toString());
        builder.environment().put("HADOOP_CONF_DIR", environment.hadoopConfDir().toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(config.logPath().toFile()));
        process = builder.start();
        try {
            Files.writeString(
                config.pidPath(),
                Long.toString(process.pid()) + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW
            );
            awaitReady();
        } catch (RuntimeException | IOException e) {
            try {
                forceKill();
            } catch (Exception stopFailure) {
                e.addSuppressed(stopFailure);
            }
            throw e;
        }
    }

    private void awaitReady() throws IOException {
        long deadline = System.nanoTime() + environment.startTimeout().toNanos();
        RuntimeException lastFailure = null;
        PmsClientConfig clientConfig = PmsClientConfig.builder(baseUri())
            .connectTimeout(Duration.ofSeconds(1))
            .readTimeout(Duration.ofSeconds(1))
            .writeTimeout(Duration.ofSeconds(1))
            .writeRetryMax(0)
            .build();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IOException(
                    "PMS process exited before ready for run " + runId
                        + ", phase=" + config.phase()
                        + ", exitCode=" + process.exitValue()
                        + ", logTail=" + logTail()
                );
            }
            try (PmsClient ignored = PmsClient.connect(clientConfig)) {
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
            }
            sleep(Duration.ofMillis(200));
        }
        throw new IOException(
            "Timed out waiting for PMS handshake for run " + runId
                + ", phase=" + config.phase()
                + ", log=" + config.logPath()
                + ", logTail=" + logTail(),
            lastFailure
        );
    }

    private void terminate(boolean force) throws IOException {
        requireStarted();
        if (stopped) {
            return;
        }
        try {
            terminateProcess(process, force, environment.operationTimeout());
        } finally {
            if (!process.isAlive()) {
                Files.deleteIfExists(config.pidPath());
                stopped = true;
            }
        }
    }

    static void terminateProcess(Process process, boolean force, Duration timeout) throws IOException {
        if (!process.isAlive()) {
            return;
        }
        if (force) {
            process.destroyForcibly();
        } else {
            process.destroy();
        }
        IOException failure;
        try {
            if (process.waitFor(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS)) {
                return;
            }
            failure = new IOException("PMS process did not stop within " + timeout + "; pid=" + process.pid());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = new IOException("Interrupted while stopping PMS process " + process.pid(), e);
        }
        // Always reap after a graceful failure, preserving the failure and interrupt status.
        boolean interrupted = Thread.interrupted();
        process.destroyForcibly();
        try {
            if (!process.waitFor(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS)) {
                failure.addSuppressed(new IOException("PMS process survived forced termination: " + process.pid()));
            }
        } catch (InterruptedException e) {
            interrupted = true;
            failure.addSuppressed(e);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        throw failure;
    }

    private String logTail() {
        try {
            if (!Files.isRegularFile(config.logPath())) {
                return "<log file not created>";
            }
            List<String> lines = Files.readAllLines(config.logPath(), StandardCharsets.UTF_8);
            int from = Math.max(0, lines.size() - 30);
            return String.join(" | ", lines.subList(from, lines.size()));
        } catch (IOException e) {
            return "<unable to read log: " + e.getMessage() + ">";
        }
    }

    private void requireStarted() {
        if (process == null) {
            throw new IllegalStateException("PMS process has not been started");
        }
    }

    private static void sleep(Duration duration) throws IOException {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for PMS process", e);
        }
    }
}
