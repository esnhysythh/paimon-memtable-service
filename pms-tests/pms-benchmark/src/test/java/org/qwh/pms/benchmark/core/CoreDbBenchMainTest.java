package org.qwh.pms.benchmark.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CoreDbBenchMainTest {
    @Test
    void allLocalCasesRunAndCleanupKeepsExistingFiles(@TempDir Path parent) throws Exception {
        Path sentinel = Files.writeString(parent.resolve("keep.txt"), "existing data");
        CoreDbBenchMain.main(new String[]{"--db=" + parent, "--num=128", "--reads=256",
            "--threads=2", "--warmup_runs=0", "--runs=1", "--delete_temp_db=true",
            "--benchmarks=fillseq,fillrandom,overwrite,readrandom,readmissing,readwhilewriting,deleterandom,flush,compact,recover"});
        assertEquals("existing data", Files.readString(sentinel));
        try (var children = Files.list(parent)) {
            assertEquals(1, children.count());
        }
    }

    @Test
    void workerFailurePreservesOriginalCause() {
        var cause = new IllegalArgumentException("injected failure");
        var failure = assertThrows(IllegalStateException.class,
            () -> CoreDbBenchMain.runConcurrent(1000, 4, 10, (op, thread) -> { throw cause; }));
        assertSame(cause, failure.getCause());
    }

    @Test
    void rejectsWrongMemoryLayerAndKeepsFailureScene(@TempDir Path parent) {
        var failure = assertThrows(IllegalStateException.class, () -> CoreDbBenchMain.main(new String[]{
            "--db=" + parent, "--benchmarks=readrandom", "--prepare=memtable", "--num=64",
            "--memtable_max_entries=16", "--warmup_runs=0", "--runs=1", "--delete_temp_db=true"}));
        assertTrue(failure.getMessage().contains("Requested prepare=MEMTABLE"));
    }

    @Test
    void rejectsLegacyDeletionFlagBeforeTouchingParent(@TempDir Path parent) throws Exception {
        Files.writeString(parent.resolve("keep.txt"), "existing data");
        assertThrows(IllegalArgumentException.class,
            () -> CoreDbBenchMain.main(new String[]{"--db=" + parent, "--fresh=true"}));
        try (var children = Files.list(parent)) {
            assertEquals(1, children.count());
        }
    }
}
