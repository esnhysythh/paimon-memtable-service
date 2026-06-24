package org.qwh.pms.core.sink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class SinkMetaPayloadCodec {
    private static final int PREPARE_MAGIC = 0x50535052; // PSPR
    private static final int SUCCESS_MAGIC = 0x50535343; // PSSC
    private static final int VERSION = 2;

    private SinkMetaPayloadCodec() {}

    public static byte[] encodePrepare(PreparedSinkCommit prepared) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(PREPARE_MAGIC);
            out.writeInt(VERSION);
            writeString(out, prepared.batchId());
            out.writeLong(prepared.commitIdentifier());
            writeLongList(out, prepared.sstIds());
            out.writeLong(prepared.minSequenceId());
            out.writeLong(prepared.maxSequenceId());
            writeBytes(out, prepared.payload());
            out.writeLong(prepared.inputRecordCount());
            out.writeLong(prepared.outputRecordCount());
            out.writeInt(prepared.fileRefs().size());
            for (SinkFileRef ref : prepared.fileRefs()) {
                writeString(out, ref.fileName());
                writeString(out, ref.path());
                out.writeLong(ref.fileSize());
                out.writeLong(ref.rowCount());
                writeString(out, ref.partition());
                out.writeInt(ref.bucket());
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encode sink prepare failed", e);
        }
    }

    public static PreparedSinkCommit decodePrepare(byte[] encoded) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != PREPARE_MAGIC || version != VERSION) {
                throw new IllegalArgumentException("unsupported sink prepare payload");
            }
            String batchId = readString(in);
            long commitIdentifier = in.readLong();
            List<Long> sstIds = readLongList(in);
            long minSequenceId = in.readLong();
            long maxSequenceId = in.readLong();
            byte[] payload = readBytes(in);
            long inputRecordCount = in.readLong();
            long outputRecordCount = in.readLong();
            int fileCount = readNonNegativeInt(in, "fileCount");
            List<SinkFileRef> refs = new ArrayList<>(fileCount);
            for (int i = 0; i < fileCount; i++) {
                refs.add(new SinkFileRef(
                    readString(in),
                    readString(in),
                    in.readLong(),
                    in.readLong(),
                    readString(in),
                    in.readInt()
                ));
            }
            requireFullyConsumed(in);
            return new PreparedSinkCommit(
                batchId,
                commitIdentifier,
                sstIds,
                minSequenceId,
                maxSequenceId,
                payload,
                refs,
                inputRecordCount,
                outputRecordCount
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("decode sink prepare failed", e);
        }
    }

    public static byte[] encodeSuccess(SinkCommitResult result) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(SUCCESS_MAGIC);
            out.writeInt(VERSION);
            writeString(out, result.batchId());
            out.writeLong(result.snapshotId());
            out.writeLong(result.persistedSequenceId());
            writeLongList(out, result.sstIds());
            writeBytes(out, result.commitPayload());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encode sink success failed", e);
        }
    }

    public static SinkCommitResult decodeSuccess(byte[] encoded) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != SUCCESS_MAGIC || version != VERSION) {
                throw new IllegalArgumentException("unsupported sink success payload");
            }
            SinkCommitResult result = new SinkCommitResult(
                readString(in),
                in.readLong(),
                in.readLong(),
                readLongList(in),
                readBytes(in)
            );
            requireFullyConsumed(in);
            return result;
        } catch (IOException e) {
            throw new IllegalArgumentException("decode sink success failed", e);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = readNonNegativeInt(in, "bytes length");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static void writeLongList(DataOutputStream out, List<Long> values) throws IOException {
        out.writeInt(values.size());
        for (long value : values) {
            out.writeLong(value);
        }
    }

    private static List<Long> readLongList(DataInputStream in) throws IOException {
        int size = readNonNegativeInt(in, "long list size");
        List<Long> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readLong());
        }
        return values;
    }

    private static int readNonNegativeInt(DataInputStream in, String name) throws IOException {
        int value = in.readInt();
        if (value < 0) {
            throw new IllegalArgumentException("invalid " + name + ": " + value);
        }
        return value;
    }

    private static void requireFullyConsumed(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IllegalArgumentException("payload has trailing bytes: " + in.available());
        }
    }
}
