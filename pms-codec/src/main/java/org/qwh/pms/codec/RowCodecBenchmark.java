package org.qwh.pms.codec;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/** Manual benchmark that sketches the row codec performance profile across common data shapes. */
public final class RowCodecBenchmark {

    private static volatile int blackholeInt;
    private static volatile Object blackholeObject;

    private RowCodecBenchmark() {}

    public static void main(String[] args) {
        int fieldCount = Math.max(1, args.length > 0 ? Integer.parseInt(args[0]) : 200);
        int iterations = Math.max(1, args.length > 1 ? Integer.parseInt(args[1]) : 100_000);

        PmsRowValueCodec codec = new PmsRowValueCodec();
        List<BenchmarkCase> cases = List.of(
                wideIntCase(fieldCount),
                nullHeavyIntCase(fieldCount),
                mixedScalarCase(Math.min(fieldCount, 120)),
                largePayloadCase(Math.min(fieldCount, 32)));

        System.out.printf("baseFields=%d baseIterations=%d%n", fieldCount, iterations);
        System.out.println(
                "case,operation,validation,projected,fields,encodedBytes,iterations,nsPerOp");
        for (BenchmarkCase benchmarkCase : cases) {
            runCase(codec, benchmarkCase, iterations);
        }
    }

    private static void runCase(PmsRowValueCodec codec, BenchmarkCase benchmarkCase, int baseIterations) {
        byte[] encoded = codec.encode(benchmarkCase.rowType(), benchmarkCase.row(), 0);
        int iterations = Math.min(baseIterations, benchmarkCase.maxIterations());
        warmUp(codec, benchmarkCase, encoded, Math.min(10_000, Math.max(1_000, iterations / 10)));

        print(
                benchmarkCase,
                "encode",
                "-",
                0,
                encoded.length,
                iterations,
                measureEncode(codec, benchmarkCase, iterations));
        print(
                benchmarkCase,
                "parse",
                "STRICT",
                0,
                encoded.length,
                iterations,
                measureParse(codec, encoded, ValidationMode.STRICT, iterations));
        print(
                benchmarkCase,
                "parse",
                "TRUSTED",
                0,
                encoded.length,
                iterations,
                measureParse(codec, encoded, ValidationMode.TRUSTED, iterations));
        print(
                benchmarkCase,
                "fullDecode",
                "STRICT",
                benchmarkCase.fieldCount(),
                encoded.length,
                iterations,
                measureFullDecode(codec, benchmarkCase, encoded, ValidationMode.STRICT, iterations));
        print(
                benchmarkCase,
                "fullDecode",
                "TRUSTED",
                benchmarkCase.fieldCount(),
                encoded.length,
                iterations,
                measureFullDecode(codec, benchmarkCase, encoded, ValidationMode.TRUSTED, iterations));

        for (int projectedCount : projectionCounts(benchmarkCase.fieldCount())) {
            int[] projectedFieldIds = evenlySpacedFieldIds(benchmarkCase.fieldCount(), projectedCount);
            print(
                    benchmarkCase,
                    "projectedDecode",
                    "STRICT",
                    projectedFieldIds.length,
                    encoded.length,
                    iterations,
                    measureProjectedDecode(
                            codec, benchmarkCase, encoded, projectedFieldIds, ValidationMode.STRICT, iterations));
            print(
                    benchmarkCase,
                    "projectedDecode",
                    "TRUSTED",
                    projectedFieldIds.length,
                    encoded.length,
                    iterations,
                    measureProjectedDecode(
                            codec, benchmarkCase, encoded, projectedFieldIds, ValidationMode.TRUSTED, iterations));
        }
    }

    private static void warmUp(
            PmsRowValueCodec codec, BenchmarkCase benchmarkCase, byte[] encoded, int warmupIterations) {
        int[] projected = evenlySpacedFieldIds(benchmarkCase.fieldCount(), Math.min(3, benchmarkCase.fieldCount()));
        int checksum = 0;
        for (int i = 0; i < warmupIterations; i++) {
            checksum += codec.encode(benchmarkCase.rowType(), benchmarkCase.row(), 0).length;
            checksum += codec.parse(encoded, ValidationMode.STRICT).payloadLength();
            checksum += codec.decode(benchmarkCase.rowType(), encoded, ValidationMode.TRUSTED).getFieldCount();
            checksum += codec.decodeProjected(
                            benchmarkCase.rowType(), encoded, projected, ValidationMode.TRUSTED)
                    .getFieldCount();
        }
        blackholeInt = checksum;
    }

    private static double measureEncode(
            PmsRowValueCodec codec, BenchmarkCase benchmarkCase, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            checksum += codec.encode(benchmarkCase.rowType(), benchmarkCase.row(), 0).length;
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureParse(
            PmsRowValueCodec codec, byte[] encoded, ValidationMode validationMode, int iterations) {
        long start = System.nanoTime();
        int checksum = 0;
        for (int i = 0; i < iterations; i++) {
            RowValueView view = codec.parse(encoded, validationMode);
            checksum += view.payloadLength();
        }
        blackholeInt = checksum;
        return nanosPerOp(start, iterations);
    }

    private static double measureFullDecode(
            PmsRowValueCodec codec,
            BenchmarkCase benchmarkCase,
            byte[] encoded,
            ValidationMode validationMode,
            int iterations) {
        long start = System.nanoTime();
        InternalRow row = null;
        for (int i = 0; i < iterations; i++) {
            row = codec.decode(benchmarkCase.rowType(), encoded, validationMode);
        }
        blackholeObject = row;
        return nanosPerOp(start, iterations);
    }

    private static double measureProjectedDecode(
            PmsRowValueCodec codec,
            BenchmarkCase benchmarkCase,
            byte[] encoded,
            int[] projectedFieldIds,
            ValidationMode validationMode,
            int iterations) {
        long start = System.nanoTime();
        InternalRow row = null;
        for (int i = 0; i < iterations; i++) {
            row = codec.decodeProjected(
                    benchmarkCase.rowType(), encoded, projectedFieldIds, validationMode);
        }
        blackholeObject = row;
        return nanosPerOp(start, iterations);
    }

    private static double nanosPerOp(long startNanos, int iterations) {
        return (System.nanoTime() - startNanos) / (double) iterations;
    }

    private static void print(
            BenchmarkCase benchmarkCase,
            String operation,
            String validation,
            int projected,
            int encodedBytes,
            int iterations,
            double nsPerOp) {
        System.out.printf(
                "%s,%s,%s,%d,%d,%d,%d,%.2f%n",
                benchmarkCase.name(),
                operation,
                validation,
                projected,
                benchmarkCase.fieldCount(),
                encodedBytes,
                iterations,
                nsPerOp);
    }

    private static BenchmarkCase wideIntCase(int fieldCount) {
        List<DataField> fields = new ArrayList<>(fieldCount);
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(DataTypes.FIELD(i + 1, "i" + i, DataTypes.INT()));
            row.setField(i, i);
        }
        return new BenchmarkCase("wide-int", new RowType(fields), row, fieldCount, Integer.MAX_VALUE);
    }

    private static BenchmarkCase nullHeavyIntCase(int fieldCount) {
        List<DataField> fields = new ArrayList<>(fieldCount);
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(DataTypes.FIELD(i + 1, "n" + i, DataTypes.INT()));
            if (i % 10 == 0) {
                row.setField(i, i);
            }
        }
        return new BenchmarkCase("null-heavy-int", new RowType(fields), row, fieldCount, Integer.MAX_VALUE);
    }

    private static BenchmarkCase mixedScalarCase(int fieldCount) {
        List<DataField> fields = new ArrayList<>(fieldCount);
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            int id = i + 1;
            switch (i % 8) {
                case 0:
                    fields.add(DataTypes.FIELD(id, "int" + i, DataTypes.INT()));
                    row.setField(i, i);
                    break;
                case 1:
                    fields.add(DataTypes.FIELD(id, "long" + i, DataTypes.BIGINT()));
                    row.setField(i, 10_000_000_000L + i);
                    break;
                case 2:
                    fields.add(DataTypes.FIELD(id, "string" + i, DataTypes.STRING()));
                    row.setField(i, BinaryString.fromString("value-" + i + "-benchmark"));
                    break;
                case 3:
                    fields.add(DataTypes.FIELD(id, "bytes" + i, DataTypes.BYTES()));
                    row.setField(i, bytes(64, i));
                    break;
                case 4:
                    fields.add(DataTypes.FIELD(id, "decimal" + i, DataTypes.DECIMAL(20, 4)));
                    row.setField(
                            i,
                            Decimal.fromBigDecimal(
                                    new BigDecimal("123456789.1234").add(BigDecimal.valueOf(i)), 20, 4));
                    break;
                case 5:
                    fields.add(DataTypes.FIELD(id, "timestamp" + i, DataTypes.TIMESTAMP(6)));
                    row.setField(i, Timestamp.fromEpochMillis(1_700_000_000_000L + i, i * 1_000));
                    break;
                case 6:
                    fields.add(DataTypes.FIELD(id, "double" + i, DataTypes.DOUBLE()));
                    row.setField(i, i + 0.25d);
                    break;
                default:
                    fields.add(DataTypes.FIELD(id, "boolean" + i, DataTypes.BOOLEAN()));
                    row.setField(i, (i & 1) == 0);
                    break;
            }
        }
        return new BenchmarkCase("mixed-scalar", new RowType(fields), row, fieldCount, Integer.MAX_VALUE);
    }

    private static BenchmarkCase largePayloadCase(int fieldCount) {
        List<DataField> fields = new ArrayList<>(fieldCount);
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(DataTypes.FIELD(i + 1, "blob" + i, DataTypes.BYTES()));
            row.setField(i, bytes(8 * 1024, i));
        }
        return new BenchmarkCase("large-payload", new RowType(fields), row, fieldCount, 10_000);
    }

    private static int[] projectionCounts(int fieldCount) {
        LinkedHashSet<Integer> counts = new LinkedHashSet<>();
        counts.add(1);
        counts.add(Math.min(3, fieldCount));
        counts.add(Math.min(10, fieldCount));
        counts.add(Math.min(50, fieldCount));
        counts.add(fieldCount);
        return counts.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] evenlySpacedFieldIds(int fieldCount, int requestedCount) {
        int count = Math.min(requestedCount, fieldCount);
        if (count <= 1) {
            return new int[] {1};
        }
        LinkedHashSet<Integer> ids = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            ids.add(1 + (int) Math.round(i * (fieldCount - 1) / (double) (count - 1)));
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }

    private static byte[] bytes(int length, int seed) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((seed + i) & 0xFF);
        }
        return bytes;
    }

    private record BenchmarkCase(
            String name, RowType rowType, GenericRow row, int fieldCount, int maxIterations) {}
}
