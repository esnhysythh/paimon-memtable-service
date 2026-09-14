package org.qwh.pms.benchmark.paimon;

import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PaimonLookupBenchMainTest {
    @Test
    void bothPathsReadTheSameCommittedFileAndNeverChangeRoute(@TempDir Path dir) throws Exception {
        var data = new LocalPaimonData(dir.resolve("table"), 32, 100);
        for (String mode : new String[]{"direct", "cached"}) {
            try (var session = new PaimonLookupBenchMain.Session(data, dir.resolve("cache"), mode)) {
                session.verify();
                for (int key = 0; key < 63; key++) {
                    var result = session.lookup(LookupRequest.fullRow(GenericRow.of(key)));
                    assertEquals(key % 2 == 0 ? LookupResult.Kind.HIT : LookupResult.Kind.MISS, result.kind());
                    if (key % 2 == 0) {
                        assertEquals(LocalPaimonData.payload(key, 100), result.row().orElseThrow().getString(1).toString());
                    }
                }
                var hit = PaimonLookupBenchMain.measure(session, new int[]{0, 14, 62, 32}, true, 2);
                var miss = PaimonLookupBenchMain.measure(session, new int[]{1, 15, 61, 33}, false, 2);
                assertEquals(mode.equals("direct") ? 4 : 0, hit.direct());
                assertEquals(mode.equals("cached") ? 4 : 0, miss.cached());
                assertThrows(Exception.class, () -> PaimonLookupBenchMain.measure(session, new int[]{1, 3}, true, 2));
            }
        }
    }

    @Test
    void cliCleansOnlyItsOwnDirectory(@TempDir Path dir) throws Exception {
        Path sentinel = Files.writeString(dir.resolve("keep.txt"), "keep");
        PaimonLookupBenchMain.main(new String[]{"--mode=cached", "--db=" + dir, "--num=16",
            "--reads=16", "--threads=2", "--warmup_runs=0", "--runs=1"});
        assertEquals("keep", Files.readString(sentinel));
        try (var children = Files.list(dir)) { assertEquals(1, children.count()); }
    }

    @Test
    void rejectsInvalidOptionsBeforePreparingData() {
        assertThrows(IllegalArgumentException.class, () -> PaimonLookupBenchMain.Config.parse(new String[]{"--mode=other"}));
        assertThrows(IllegalArgumentException.class, () -> PaimonLookupBenchMain.Config.parse(new String[]{"--num=1"}));
        assertThrows(IllegalArgumentException.class, () -> PaimonLookupBenchMain.Config.parse(new String[]{"--fresh=true"}));
    }
}
