package org.qwh.pms.testkit.environment;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public record PmsTestEnvironment(
        Path pmsJavaHome,
        Path hadoopConfDir,
        Path localRoot,
        URI hdfsRoot,
        String serverClasspath,
        CleanupPolicy cleanupPolicy,
        Duration startTimeout,
        Duration operationTimeout) {

    public static final String PMS_JAVA_HOME = "pms.it.pms.java.home";
    public static final String HADOOP_CONF_DIR = "pms.it.hadoop.conf.dir";
    public static final String LOCAL_ROOT = "pms.it.local.root";
    public static final String HDFS_ROOT = "pms.it.hdfs.root";
    public static final String SERVER_CLASSPATH = "pms.it.server.classpath";
    public static final String CLEANUP = "pms.it.cleanup";
    public static final String START_TIMEOUT = "pms.it.start.timeout";
    public static final String OPERATION_TIMEOUT = "pms.it.operation.timeout";

    private static final Pattern JAVA_17 = Pattern.compile(
        "(?m)^\\s*java\\.specification\\.version\\s*=\\s*17\\s*$"
    );

    public PmsTestEnvironment {
        pmsJavaHome = normalizeAbsolute(pmsJavaHome, PMS_JAVA_HOME);
        hadoopConfDir = normalizeAbsolute(hadoopConfDir, HADOOP_CONF_DIR);
        localRoot = validateLocalRoot(localRoot);
        hdfsRoot = validateHdfsRoot(hdfsRoot);
        serverClasspath = requireNonBlank(serverClasspath, SERVER_CLASSPATH);
        cleanupPolicy = Objects.requireNonNull(cleanupPolicy, "cleanupPolicy must not be null");
        startTimeout = requirePositive(startTimeout, START_TIMEOUT);
        operationTimeout = requirePositive(operationTimeout, OPERATION_TIMEOUT);

        requireReadableFile(hadoopConfDir.resolve("core-site.xml"), "core-site.xml");
        requireReadableFile(hadoopConfDir.resolve("hdfs-site.xml"), "hdfs-site.xml");
        requireExecutable(pmsJavaHome.resolve("bin").resolve("java"));
    }

    public static PmsTestEnvironment fromSystemProperties() {
        return fromProperties(System.getProperties(), true);
    }

    static PmsTestEnvironment fromProperties(Properties properties, boolean verifyJavaVersion) {
        Objects.requireNonNull(properties, "properties must not be null");
        String classpath = firstNonBlank(
            properties.getProperty(SERVER_CLASSPATH),
            properties.getProperty("surefire.test.class.path"),
            properties.getProperty("java.class.path")
        );
        PmsTestEnvironment environment = new PmsTestEnvironment(
            Path.of(required(properties, PMS_JAVA_HOME)),
            Path.of(required(properties, HADOOP_CONF_DIR)),
            Path.of(required(properties, LOCAL_ROOT)),
            URI.create(required(properties, HDFS_ROOT)),
            classpath,
            CleanupPolicy.parse(properties.getProperty(CLEANUP, "on-success")),
            parseDuration(properties.getProperty(START_TIMEOUT, "60s"), START_TIMEOUT),
            parseDuration(properties.getProperty(OPERATION_TIMEOUT, "120s"), OPERATION_TIMEOUT)
        );
        if (verifyJavaVersion) {
            environment.verifyJava17();
        }
        return environment;
    }

    public Path javaExecutable() {
        return pmsJavaHome.resolve("bin").resolve("java");
    }

    private void verifyJava17() {
        Process process;
        try {
            process = new ProcessBuilder(
                javaExecutable().toString(),
                "-XshowSettings:properties",
                "-version"
            ).redirectErrorStream(true).start();
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to run PMS Java executable: " + javaExecutable(), e);
        }
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalArgumentException("Timed out while checking PMS Java: " + javaExecutable());
            }
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (process.exitValue() != 0 || !JAVA_17.matcher(output).find()) {
                throw new IllegalArgumentException(
                    PMS_JAVA_HOME + " must point to a Java 17 home: " + pmsJavaHome
                );
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read PMS Java version output", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while checking PMS Java version", e);
        }
    }

    private static Path validateLocalRoot(Path value) {
        Path root = normalizeAbsolute(value, LOCAL_ROOT);
        Path filesystemRoot = root.getRoot();
        if (root.equals(filesystemRoot)) {
            throw new IllegalArgumentException(LOCAL_ROOT + " must not be a filesystem root: " + root);
        }
        String userHome = System.getProperty("user.home");
        if (userHome != null && root.equals(Path.of(userHome).toAbsolutePath().normalize())) {
            throw new IllegalArgumentException(LOCAL_ROOT + " must not be the user home directory: " + root);
        }
        return root;
    }

    private static URI validateHdfsRoot(URI value) {
        Objects.requireNonNull(value, HDFS_ROOT + " must not be null");
        if (!"hdfs".equalsIgnoreCase(value.getScheme())) {
            throw new IllegalArgumentException(HDFS_ROOT + " must use hdfs scheme: " + value);
        }
        if (value.getAuthority() == null || value.getAuthority().isBlank()) {
            throw new IllegalArgumentException(HDFS_ROOT + " must include a NameNode authority: " + value);
        }
        if (value.getQuery() != null || value.getFragment() != null) {
            throw new IllegalArgumentException(HDFS_ROOT + " must not contain query or fragment: " + value);
        }
        for (String segment : value.getPath().split("/")) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(
                    HDFS_ROOT + " must not contain dot path segments: " + value
                );
            }
        }
        String path = normalizeUriPath(value.getPath());
        if (path.equals("/") || path.equals("/tmp") || path.equals("/user")) {
            throw new IllegalArgumentException(HDFS_ROOT + " must be a dedicated non-root path: " + value);
        }
        return URI.create("hdfs://" + value.getAuthority() + path);
    }

    private static String normalizeUriPath(String value) {
        if (value == null || value.isBlank()) {
            return "/";
        }
        String normalized = value.replaceAll("/{2,}", "/");
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    private static Path normalizeAbsolute(Path value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (!value.isAbsolute()) {
            throw new IllegalArgumentException(name + " must be an absolute path: " + value);
        }
        return value.normalize();
    }

    private static void requireReadableFile(Path path, String description) {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(description + " is missing or unreadable: " + path);
        }
    }

    private static void requireExecutable(Path path) {
        if (!Files.isRegularFile(path) || !Files.isExecutable(path)) {
            throw new IllegalArgumentException("PMS Java executable is missing or not executable: " + path);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
        return value;
    }

    static Duration parseDuration(String value, String name) {
        String raw = requireNonBlank(value, name);
        String normalized = raw.toLowerCase(Locale.ROOT);
        try {
            if (normalized.startsWith("p")) {
                return Duration.parse(raw.toUpperCase(Locale.ROOT));
            }
            if (normalized.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(normalized.substring(0, normalized.length() - 2)));
            }
            if (normalized.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(normalized.substring(0, normalized.length() - 1)));
            }
            if (normalized.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(normalized.substring(0, normalized.length() - 1)));
            }
            return Duration.parse(raw);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(name + " is not a valid duration: " + value, e);
        }
    }

    private static String required(Properties properties, String name) {
        return requireNonBlank(properties.getProperty(name), name);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + name);
        }
        return value.trim();
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        throw new IllegalArgumentException("Missing required property: " + SERVER_CLASSPATH);
    }
}
