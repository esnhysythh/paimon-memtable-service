package org.qwh.pms.lookup.key;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import java.util.Arrays;
import java.util.Comparator;

/** Describes lookup key fields and their Paimon comparison semantics. */
public final class LookupKeySpec {

    private final RowType tableRowType;
    private final int[] keyFieldIndexes;
    private final RowType keyType;
    private final Comparator<InternalRow> keyComparator;

    private LookupKeySpec(RowType tableRowType, int[] keyFieldIndexes) {
        this.tableRowType = tableRowType;
        this.keyFieldIndexes = keyFieldIndexes.clone();
        this.keyType = tableRowType.project(keyFieldIndexes);
        this.keyComparator = new KeyComparatorSupplier(keyType).get();
    }

    public static LookupKeySpec of(RowType tableRowType, int... keyFieldIndexes) {
        if (keyFieldIndexes == null || keyFieldIndexes.length == 0) {
            throw new IllegalArgumentException("Lookup key must contain at least one field.");
        }
        for (int index : keyFieldIndexes) {
            if (index < 0 || index >= tableRowType.getFieldCount()) {
                throw new IllegalArgumentException(
                        "Key field index out of table row bounds: "
                                + index
                                + ", rowType="
                                + tableRowType);
            }
        }
        return new LookupKeySpec(tableRowType, keyFieldIndexes);
    }

    public RowType tableRowType() {
        return tableRowType;
    }

    public int[] keyFieldIndexes() {
        return keyFieldIndexes.clone();
    }

    public int keyFieldIndex(int keyOrdinal) {
        return keyFieldIndexes[keyOrdinal];
    }

    public int keyFieldCount() {
        return keyFieldIndexes.length;
    }

    public RowType keyType() {
        return keyType;
    }

    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    @Override
    public String toString() {
        return "LookupKeySpec{"
                + "keyFieldIndexes="
                + Arrays.toString(keyFieldIndexes)
                + ", keyType="
                + keyType
                + '}';
    }
}
