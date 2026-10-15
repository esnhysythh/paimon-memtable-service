package org.qwh.pms.core.wal;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.PaimonConfig;
import org.qwh.pms.core.config.WalConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WalProcessRecoveryTest {
    @TempDir Path dir;

    private static PMSConfig config(Path dir, boolean mmap) {
        return new PMSConfig(null, new WalConfig(dir.toString(), 256, mmap),
            null, null, null, new PaimonConfig("dummy", null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void acknowledgedRecordsSurviveProcessKillWithoutWriterClose(boolean mmap) throws Exception {
        Path ready = dir.resolve("ready");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"), WriterProcess.class.getName(),
            dir.toString(), Boolean.toString(mmap))
            .redirectErrorStream(true).redirectOutput(dir.resolve("child.log").toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!Files.exists(ready) && child.isAlive() && System.nanoTime() < deadline) Thread.sleep(10);
            assertTrue(Files.exists(ready), () -> "writer did not become ready: " + child.info());
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS));
            WALManagerImpl wal = new WALManagerImpl(config(dir, mmap));
            WALManagerImplTest.CollectingCallback cb = new WALManagerImplTest.CollectingCallback();
            wal.recover(0, cb);
            try {
                assertEquals(4, cb.dataRecords.size());
                assertArrayEquals(new byte[]{2}, cb.dataRecords.get(1).value());
                assertNull(cb.dataRecords.get(2).value());
                assertEquals(100_000, cb.dataRecords.get(3).value().length);
                assertEquals(5, wal.appendDataRecord(new byte[]{3}, null));
            } finally {
                wal.close();
            }
            WALManagerImpl again = new WALManagerImpl(config(dir, mmap));
            again.recover(0, (k, v) -> {});
            assertEquals(5, again.lastSequenceId());
            again.close();
        } finally {
            child.destroyForcibly();
            child.waitFor(10, TimeUnit.SECONDS);
        }
    }

    public static final class WriterProcess {
        public static void main(String[] args) throws Exception {
            Path dir = Path.of(args[0]);
            WALManagerImpl wal = new WALManagerImpl(config(dir, Boolean.parseBoolean(args[1])));
            wal.recover(0, (k, v) -> {});
            wal.appendDataRecord(new byte[]{1}, new byte[]{1});
            wal.appendDataRecords(List.of(new WALManager.DataWrite(new byte[]{1}, new byte[]{2}),
                new WALManager.DataWrite(new byte[]{1}, null),
                new WALManager.DataWrite(new byte[]{2}, new byte[100_000])));
            Files.writeString(dir.resolve("ready"), "4");
            // No close/shutdown hook: the parent kills this process with both writers still live.
            Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        }
    }
}
