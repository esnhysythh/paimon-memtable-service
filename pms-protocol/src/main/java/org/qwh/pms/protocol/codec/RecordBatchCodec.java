package org.qwh.pms.protocol.codec;

import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RecordOp;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class RecordBatchCodec {

    private RecordBatchCodec() {}

    public static byte[] encodeRequest(List<RawKvEntry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("record batch must not be empty");
        }
        BinaryPayloadWriter writer = new BinaryPayloadWriter();
        VarInt.writeUnsignedInt(writer, entries.size());
        for (RawKvEntry entry : entries) {
            writer.writeBytes(encodeRecord(entry));
        }
        return writer.toByteArray();
    }

    public static byte[] encodeSinglePut(byte[] key, byte[] row) {
        return encodeRequest(List.of(RawKvEntry.put(key, row)));
    }

    public static byte[] encodeSingleDelete(byte[] key) {
        return encodeRequest(List.of(RawKvEntry.delete(key)));
    }

    public static List<RawKvEntry> decodeRequest(byte[] body) {
        return decodeRequest(body, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static List<RawKvEntry> decodeRequest(byte[] body, int maxEntries, int maxKeyBytes, int maxRowBytes) {
        requirePositive(maxEntries, "maxEntries");
        requirePositive(maxKeyBytes, "maxKeyBytes");
        requirePositive(maxRowBytes, "maxRowBytes");
        BinaryPayloadReader reader = new BinaryPayloadReader(body);
        int count = VarInt.readUnsignedInt(reader);
        if (count == 0) {
            throw new IllegalArgumentException("record count must be positive");
        }
        if (count > maxEntries) {
            throw new IllegalArgumentException("record count exceeds limit: " + count + " > " + maxEntries);
        }
        List<RawKvEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(decodeRecord(reader, maxKeyBytes, maxRowBytes));
        }
        reader.requireFullyRead();
        return List.copyOf(entries);
    }

    private static byte[] encodeRecord(RawKvEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        BinaryPayloadWriter record = new BinaryPayloadWriter();
        VarInt.writeUnsignedInt(record, entry.isDelete() ? RecordOp.DELETE.code() : RecordOp.PUT.code());
        VarInt.writeUnsignedInt(record, entry.key().length);
        record.writeBytes(entry.key());
        if (!entry.isDelete()) {
            record.writeBytes(entry.row());
        }
        byte[] recordBytes = record.toByteArray();
        return new BinaryPayloadWriter(Integer.BYTES + recordBytes.length)
                .writeInt(recordBytes.length)
                .writeBytes(recordBytes)
                .toByteArray();
    }

    private static RawKvEntry decodeRecord(BinaryPayloadReader reader, int maxKeyBytes, int maxRowBytes) {
        int recordLength = reader.readInt();
        if (recordLength < 0) {
            throw new IllegalArgumentException("recordLength must not be negative: " + recordLength);
        }
        long maxRecordLength = 10L + maxKeyBytes + maxRowBytes;
        if (recordLength > maxRecordLength) {
            throw new IllegalArgumentException("recordLength exceeds limit: " + recordLength + " > " + maxRecordLength);
        }
        BinaryPayloadReader record = new BinaryPayloadReader(reader.readBytes(recordLength));
        RecordOp op = RecordOp.fromCode(VarInt.readUnsignedInt(record));
        int keyLength = VarInt.readUnsignedInt(record);
        if (keyLength == 0) {
            throw new IllegalArgumentException("record key must not be empty");
        }
        if (keyLength > maxKeyBytes) {
            throw new IllegalArgumentException("key too large: " + keyLength + " > " + maxKeyBytes);
        }
        byte[] key = record.readBytes(keyLength);
        RawKvEntry entry;
        if (op == RecordOp.PUT) {
            int rowLength = record.remaining();
            if (rowLength > maxRowBytes) {
                throw new IllegalArgumentException("row too large: " + rowLength + " > " + maxRowBytes);
            }
            entry = RawKvEntry.put(key, record.readBytes(rowLength));
        } else {
            if (record.remaining() != 0) {
                throw new IllegalArgumentException("DELETE record must not contain row bytes");
            }
            entry = RawKvEntry.delete(key);
        }
        record.requireFullyRead();
        return entry;
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
