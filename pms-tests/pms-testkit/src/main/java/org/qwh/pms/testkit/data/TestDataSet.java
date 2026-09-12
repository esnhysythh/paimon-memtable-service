package org.qwh.pms.testkit.data;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record TestDataSet(
        long seed,
        List<TestRecord> inserts,
        List<TestRecord> updates,
        List<Long> deletes,
        Map<Long, TestRecord> expected) {

    public TestDataSet {
        inserts = List.copyOf(inserts);
        updates = List.copyOf(updates);
        deletes = List.copyOf(deletes);
        expected = Collections.unmodifiableMap(new LinkedHashMap<>(expected));
    }
}
