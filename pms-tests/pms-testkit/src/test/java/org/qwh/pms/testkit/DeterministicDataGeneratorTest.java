package org.qwh.pms.testkit;

import org.junit.jupiter.api.Test;
import org.qwh.pms.testkit.data.DeterministicDataGenerator;
import org.qwh.pms.testkit.data.TestDataSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DeterministicDataGeneratorTest {

    @Test
    void sameSeedProducesSameScenario() {
        TestDataSet first = DeterministicDataGenerator.standardScenario(100, 32, 8128L);
        TestDataSet second = DeterministicDataGenerator.standardScenario(100, 32, 8128L);

        assertEquals(first, second);
        assertEquals(100, first.inserts().size());
        assertEquals(20, first.updates().size());
        assertEquals(9, first.deletes().size());
    }

    @Test
    void seedAndVersionAffectPayload() {
        assertNotEquals(
            DeterministicDataGenerator.record(7, 0, 64, 1).payload(),
            DeterministicDataGenerator.record(7, 1, 64, 1).payload()
        );
        assertNotEquals(
            DeterministicDataGenerator.record(7, 0, 64, 1).payload(),
            DeterministicDataGenerator.record(7, 0, 64, 2).payload()
        );
    }
}
