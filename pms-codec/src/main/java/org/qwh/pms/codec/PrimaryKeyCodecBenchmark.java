package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Manual benchmark for PMS primary key encoding and prefix-range helpers. */
public final class PrimaryKeyCodecBenchmark {

    private static final int ROWS_PER_CASE = 1_024;

    private static volatile int blackholeInt;
    private static volatile Object blackholeObject;

    private PrimaryKeyCodecBenchmark() {}

    public static void main(String[] args) {
        int iterations = Math.max(1, args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000);
        List<BenchmarkCase> cases =
                List.of(
                        singleIntCase(),
                        intStringCase(),
                        temporalCase(),
                        bytesHeavyCase(),
                        sparseWideTableCase());

        System.out.printf("baseIterations=%d rowsPerCase=%d%n", iterations, ROWS_PER_CASE);
        System.out.println("case,operation,pkFields,rowFields,keyBytes,iterations,nsPerOp");
        for (BenchmarkCase benchmarkCase : cases) {
            runCase(benchmarkCase, iterations);
        }
    }

    private static void runCase(BenchmarkCase benchmarkCase, int baseIterations) {
        PmsPrimaryKeyCodec codec =
                new PmsPrimaryKeyCodec(benchmarkCase.rowType(), benchmarkCase.primaryKeyFieldIds());
        byte[][] encodedKeys = encodedKeys(codec, benchmarkCase.rows());
        byte[][] encodedPrefixes = encodedPrefixes(codec, benchmarkCase);
        int iterations = Math.min(baseIterations, benchmarkCase.maxIterations());
        warmUp(
                codec,
                benchmarkCase,
                encodedKeys,
                encodedPrefixes,
                Math.min(20_000, Math.max(2_000, iterations / 10)));

        print(
                benchmarkCase,
                "encodeKey",
                encodedKeys[0].length,
                iterations,
                measureEncodeKey(codec, benchmarkCase, iterations));
        print(
                benchmarkCase,
                "encodeKeyTuple",
                encodedKeys[0].length,
                iterations,
                measureEncodeKeyTuple(codec, benchmarkCase, iterations));
        print(
                benchmarkCase,
                "decodeKey",
                encodedKeys[0].length,
                iterations,
                measureDecodeKey(codec, encodedKeys, iterations));
        print(
                benchmarkCase,
                "encodePrefix",
                encodedPrefixes[0].length,
                iterations,
                measureEncodePrefix(codec, benchmarkCase, iterations));
        print(
                benchmarkCase,
                "decodePrefix",
                encodedPrefixes[0].length,
                iterations,
                measureDecodePrefix(codec, benchmarkCase, encodedPrefixes, iterations));
        print(
                benchmarkCase,
                "prefixNext",
                encodedPrefixes[0].length,
                iterations,
                measurePrefixNext(encodedPrefixes, iterations));
        print(
                benchmarkCase,
                "unsignedCompare",
                encodedKeys[0].length,
                iterations,
                measureUnsignedCompare(encodedKeys, iterations));
    }

    private static void warmUp(
            PmsPrimaryKeyCodec codec,
            BenchmarkCase benchmarkCase,
            byte[][] encodedKeys,
            byte[][] encodedPrefixes,
            int warmupIterations) {
        int checksum = 0;
        for (int i = 0; i < warmupIterations; i++) {
            int index = i & (benchmarkCase.rows().size() - 1);
            byte[] key = codec.encodeKey(benchmarkCase.rows().get(index));
            checksum += key.length;
            checksum += codec.encodeKeyTuple(benchmarkCase.keyTuples().get(index)).length;
            checksum += codec.decodeKey(encodedKeys[index]).getFieldCount();
            byte[] prefix = codec.encodePrefix(
                    benchmarkCase.rows().get(index), benchmarkCase.prefixFieldCount());
            checksum += prefix.length;
            checksum += codec.decodePrefix(encodedPrefixes[index], benchmarkCase.prefixFieldCount()).getFieldCount();
            checksum += PmsPrimaryKeyCodec.prefixNext(encodedPrefixes[index]).map(bytes -> bytes.length).orElse(0);
            checksum += unsignedCompare(encodedKeys[index], encodedKeys[(index + 1) & (encodedKeys.length - 1)]);
        }
        blackholeInt = checksum;
    }

    private static double measureEncodeKey(
            PmsPrimaryKeyCodec codec, BenchmarkCase benchmarkCase, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            checksum += codec.encodeKey(benchmarkCase.rows().get(i & (benchmarkCase.rows().size() - 1))).length;
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureEncodeKeyTuple(
            PmsPrimaryKeyCodec codec, BenchmarkCase benchmarkCase, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            checksum += codec.encodeKeyTuple(
                            benchmarkCase.keyTuples().get(i & (benchmarkCase.keyTuples().size() - 1)))
                    .length;
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureDecodeKey(
            PmsPrimaryKeyCodec codec, byte[][] encodedKeys, int iterations) {
        long start = System.nanoTime();
        InternalRow row = null;
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            row = codec.decodeKey(encodedKeys[i & (encodedKeys.length - 1)]);
            checksum += row.getFieldCount();
        }
        blackholeObject = row;
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureEncodePrefix(
            PmsPrimaryKeyCodec codec, BenchmarkCase benchmarkCase, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            checksum += codec.encodePrefix(
                            benchmarkCase.rows().get(i & (benchmarkCase.rows().size() - 1)),
                            benchmarkCase.prefixFieldCount())
                    .length;
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureDecodePrefix(
            PmsPrimaryKeyCodec codec,
            BenchmarkCase benchmarkCase,
            byte[][] encodedPrefixes,
            int iterations) {
        long start = System.nanoTime();
        InternalRow row = null;
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            row = codec.decodePrefix(
                    encodedPrefixes[i & (encodedPrefixes.length - 1)], benchmarkCase.prefixFieldCount());
            checksum += row.getFieldCount();
        }
        blackholeObject = row;
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measurePrefixNext(byte[][] prefixes, int iterations) {
        long start = System.nanoTime();
        Optional<byte[]> result = Optional.empty();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            result = PmsPrimaryKeyCodec.prefixNext(prefixes[i & (prefixes.length - 1)]);
            checksum += result.map(bytes -> bytes.length).orElse(0);
        }
        blackholeObject = result;
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureUnsignedCompare(byte[][] encodedKeys, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            int left = i & (encodedKeys.length - 1);
            int right = (i + 1) & (encodedKeys.length - 1);
            checksum += unsignedCompare(encodedKeys[left], encodedKeys[right]);
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static byte[][] encodedKeys(PmsPrimaryKeyCodec codec, List<InternalRow> rows) {
        byte[][] keys = new byte[rows.size()][];
        for (int i = 0; i < rows.size(); i++) {
            keys[i] = codec.encodeKey(rows.get(i));
        }
        return keys;
    }

    private static byte[][] encodedPrefixes(PmsPrimaryKeyCodec codec, BenchmarkCase benchmarkCase) {
        byte[][] prefixes = new byte[benchmarkCase.rows().size()][];
        for (int i = 0; i < prefixes.length; i++) {
            prefixes[i] = codec.encodePrefix(benchmarkCase.rows().get(i), benchmarkCase.prefixFieldCount());
        }
        return prefixes;
    }

    private static void print(
            BenchmarkCase benchmarkCase,
            String operation,
            int encodedBytes,
            int iterations,
            double nsPerOp) {
        System.out.printf(
                "%s,%s,%d,%d,%d,%d,%.2f%n",
                benchmarkCase.name(),
                operation,
                benchmarkCase.primaryKeyFieldIds().length,
                benchmarkCase.rowType().getFieldCount(),
                encodedBytes,
                iterations,
                nsPerOp);
    }

    private static double nanosPerOp(long startNanos, int iterations) {
        return (System.nanoTime() - startNanos) / (double) iterations;
    }

    private static BenchmarkCase singleIntCase() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "payload", DataTypes.STRING()));
        List<InternalRow> rows = new ArrayList<>(ROWS_PER_CASE);
        List<InternalRow> tuples = new ArrayList<>(ROWS_PER_CASE);
        for (int i = 0; i < ROWS_PER_CASE; i++) {
            int id = i - ROWS_PER_CASE / 2;
            rows.add(GenericRow.of(id, BinaryString.fromString("payload-" + i)));
            tuples.add(GenericRow.of(id));
        }
        return new BenchmarkCase("single-int", rowType, new int[] {1}, rows, tuples, 1, Integer.MAX_VALUE);
    }

    private static BenchmarkCase intStringCase() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "tenant", DataTypes.INT()),
                        DataTypes.FIELD(2, "user_id", DataTypes.STRING()),
                        DataTypes.FIELD(3, "payload", DataTypes.BIGINT()));
        List<InternalRow> rows = new ArrayList<>(ROWS_PER_CASE);
        List<InternalRow> tuples = new ArrayList<>(ROWS_PER_CASE);
        for (int i = 0; i < ROWS_PER_CASE; i++) {
            int tenant = i % 128;
            BinaryString userId = BinaryString.fromString("user-" + tenant + "-" + i);
            rows.add(GenericRow.of(tenant, userId, 10_000_000L + i));
            tuples.add(GenericRow.of(tenant, userId));
        }
        return new BenchmarkCase("int-string", rowType, new int[] {1, 2}, rows, tuples, 1, Integer.MAX_VALUE);
    }

    private static BenchmarkCase temporalCase() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "day", DataTypes.DATE()),
                        DataTypes.FIELD(2, "time", DataTypes.TIME()),
                        DataTypes.FIELD(3, "ts3", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD(4, "ts6", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD(5, "payload", DataTypes.STRING()));
        List<InternalRow> rows = new ArrayList<>(ROWS_PER_CASE);
        List<InternalRow> tuples = new ArrayList<>(ROWS_PER_CASE);
        for (int i = 0; i < ROWS_PER_CASE; i++) {
            int day = i - 512;
            int time = (i * 997) % 86_400_000;
            Timestamp ts3 = Timestamp.fromEpochMillis(1_700_000_000_000L + i);
            Timestamp ts6 = Timestamp.fromEpochMillis(1_700_000_000_000L + i, (i * 1_000) % 1_000_000);
            rows.add(GenericRow.of(day, time, ts3, ts6, BinaryString.fromString("payload-" + i)));
            tuples.add(GenericRow.of(day, time, ts3, ts6));
        }
        return new BenchmarkCase("temporal", rowType, new int[] {1, 2, 3, 4}, rows, tuples, 2, Integer.MAX_VALUE);
    }

    private static BenchmarkCase bytesHeavyCase() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "bucket", DataTypes.INT()),
                        DataTypes.FIELD(2, "raw", DataTypes.BYTES()),
                        DataTypes.FIELD(3, "suffix", DataTypes.STRING()));
        List<InternalRow> rows = new ArrayList<>(ROWS_PER_CASE);
        List<InternalRow> tuples = new ArrayList<>(ROWS_PER_CASE);
        for (int i = 0; i < ROWS_PER_CASE; i++) {
            int bucket = i % 64;
            byte[] raw = bytes(128, i);
            BinaryString suffix = BinaryString.fromString("suffix-" + i);
            rows.add(GenericRow.of(bucket, raw, suffix));
            tuples.add(GenericRow.of(bucket, raw, suffix));
        }
        return new BenchmarkCase("bytes-heavy", rowType, new int[] {1, 2, 3}, rows, tuples, 1, 200_000);
    }

    private static BenchmarkCase sparseWideTableCase() {
        int fieldCount = 120;
        List<DataField> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            int fieldId = i + 1;
            if (fieldId == 7) {
                fields.add(DataTypes.FIELD(fieldId, "pk_a", DataTypes.INT()));
            } else if (fieldId == 43) {
                fields.add(DataTypes.FIELD(fieldId, "pk_b", DataTypes.STRING()));
            } else if (fieldId == 119) {
                fields.add(DataTypes.FIELD(fieldId, "pk_c", DataTypes.BIGINT()));
            } else {
                fields.add(DataTypes.FIELD(fieldId, "payload_" + fieldId, DataTypes.INT()));
            }
        }
        RowType rowType = new RowType(fields);
        List<InternalRow> rows = new ArrayList<>(ROWS_PER_CASE);
        List<InternalRow> tuples = new ArrayList<>(ROWS_PER_CASE);
        for (int i = 0; i < ROWS_PER_CASE; i++) {
            GenericRow row = new GenericRow(fieldCount);
            for (int field = 0; field < fieldCount; field++) {
                row.setField(field, i + field);
            }
            int pkA = i - 512;
            BinaryString pkB = BinaryString.fromString("wide-" + i);
            long pkC = 1_000_000_000L + i;
            row.setField(6, pkA);
            row.setField(42, pkB);
            row.setField(118, pkC);
            rows.add(row);
            tuples.add(GenericRow.of(pkA, pkB, pkC));
        }
        return new BenchmarkCase("sparse-wide-table", rowType, new int[] {7, 43, 119}, rows, tuples, 2, Integer.MAX_VALUE);
    }

    private static byte[] bytes(int length, int seed) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((seed + i) & 0xFF);
        }
        return bytes;
    }

    private static int unsignedCompare(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int cmp = (left[i] & 0xFF) - (right[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private record BenchmarkCase(
            String name,
            RowType rowType,
            int[] primaryKeyFieldIds,
            List<InternalRow> rows,
            List<InternalRow> keyTuples,
            int prefixFieldCount,
            int maxIterations) {}
}
