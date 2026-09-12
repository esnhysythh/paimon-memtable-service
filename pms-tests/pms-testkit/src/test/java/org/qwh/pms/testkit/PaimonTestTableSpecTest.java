package org.qwh.pms.testkit;

import org.junit.jupiter.api.Test;
import org.qwh.pms.testkit.paimon.PaimonTestTableSpec;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PaimonTestTableSpecTest {

    @Test
    void derivesSafeIdentifiersAndExpectedSchema() {
        PaimonTestTableSpec spec = PaimonTestTableSpec.forRun(
            "20260811-ABCD-1234",
            "Remote HDFS Smoke"
        );

        assertEquals("pms_it_r_20260811_abcd_1234", spec.database());
        assertEquals("remote_hdfs_smoke", spec.table());
        assertEquals(List.of("id"), spec.schema().primaryKeys());
        assertEquals("1", spec.schema().options().get("bucket"));
        assertFalse(spec.schema().fields().get(0).type().isNullable());
    }
}
