package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.memtable.model.Entry;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

final class BlockReaders {
    private BlockReaders() {
    }

    static List<IndexEntry> readIndexBlock(byte[] block) {
        ParsedBlock parsed = parseBlockTail(block);
        List<IndexEntry> entries = new ArrayList<>();
        byte[] previousKey = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(block, 0, parsed.dataEnd));
        while (in.available() > 0) {
            int shared = StorageCoding.readVarInt(in);
            int unshared = StorageCoding.readVarInt(in);
            int valueLen = StorageCoding.readVarInt(in);
            byte[] keyBytes = reconstructKey(previousKey, shared, readBytes(in, unshared));
            byte[] value = readBytes(in, valueLen);
            BlockHandle handle = decodeUnpaddedHandle(value);
            entries.add(new IndexEntry(new Key(keyBytes), handle));
            previousKey = keyBytes;
        }
        return entries;
    }

    static Optional<Value> findInDataBlock(byte[] block, Key target) {
        ParsedBlock parsed = parseBlockTail(block);
        byte[] previousKey = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(block, 0, parsed.dataEnd));
        while (in.available() > 0) {
            int shared = StorageCoding.readVarInt(in);
            int unshared = StorageCoding.readVarInt(in);
            byte[] header = readBytes(in, 12);
            long sequenceId = StorageCoding.readLongLE(header, 0);
            int valueLen = StorageCoding.readIntLE(header, 8);
            byte[] keyBytes = reconstructKey(previousKey, shared, readBytes(in, unshared));
            Key key = new Key(keyBytes);
            if (valueLen == SSTFormat.VALUE_LEN_DELETE) {
                if (key.compareTo(target) == 0) {
                    return Optional.of(Value.tombstone(sequenceId));
                }
            } else {
                if (valueLen < 0) {
                    throw new IllegalArgumentException("invalid valueLen: " + valueLen);
                }
                byte[] value = readBytes(in, valueLen);
                if (key.compareTo(target) == 0) {
                    return Optional.of(new Value(value, sequenceId));
                }
            }
            if (key.compareTo(target) > 0) {
                return Optional.empty();
            }
            previousKey = keyBytes;
        }
        return Optional.empty();
    }

    static List<Entry> readDataBlockEntries(byte[] block) {
        ParsedBlock parsed = parseBlockTail(block);
        List<Entry> entries = new ArrayList<>();
        byte[] previousKey = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(block, 0, parsed.dataEnd));
        while (in.available() > 0) {
            int shared = StorageCoding.readVarInt(in);
            int unshared = StorageCoding.readVarInt(in);
            byte[] header = readBytes(in, 12);
            long sequenceId = StorageCoding.readLongLE(header, 0);
            int valueLen = StorageCoding.readIntLE(header, 8);
            byte[] keyBytes = reconstructKey(previousKey, shared, readBytes(in, unshared));
            Key key = new Key(keyBytes);
            Value value;
            if (valueLen == SSTFormat.VALUE_LEN_DELETE) {
                value = Value.tombstone(sequenceId);
            } else {
                if (valueLen < 0) {
                    throw new IllegalArgumentException("invalid valueLen: " + valueLen);
                }
                value = new Value(readBytes(in, valueLen), sequenceId);
            }
            entries.add(new Entry(key, value));
            previousKey = keyBytes;
        }
        return entries;
    }

    private static BlockHandle decodeUnpaddedHandle(byte[] value) {
        byte[] padded = new byte[BlockHandle.MAX_ENCODED_LENGTH];
        System.arraycopy(value, 0, padded, 0, Math.min(value.length, padded.length));
        return StorageCoding.decodeHandle(padded, 0);
    }

    private static ParsedBlock parseBlockTail(byte[] block) {
        if (block.length < 4) {
            throw new IllegalArgumentException("block too short");
        }
        int restartCount = StorageCoding.readIntLE(block, block.length - 4);
        if (restartCount <= 0) {
            throw new IllegalArgumentException("invalid restart count: " + restartCount);
        }
        int dataEnd = block.length - 4 - restartCount * 4;
        if (dataEnd < 0) {
            throw new IllegalArgumentException("invalid restart offsets");
        }
        return new ParsedBlock(dataEnd);
    }

    private static byte[] reconstructKey(byte[] previousKey, int shared, byte[] unshared) {
        if (shared < 0 || shared > previousKey.length) {
            throw new IllegalArgumentException("invalid shared key length");
        }
        byte[] key = new byte[shared + unshared.length];
        System.arraycopy(previousKey, 0, key, 0, shared);
        System.arraycopy(unshared, 0, key, shared, unshared.length);
        return key;
    }

    private static byte[] readBytes(ByteArrayInputStream in, int length) {
        if (length < 0 || in.available() < length) {
            throw new IllegalArgumentException("truncated block entry");
        }
        byte[] data = new byte[length];
        int read = in.read(data, 0, length);
        if (read != length) {
            throw new IllegalArgumentException("truncated block entry");
        }
        return data;
    }

    record IndexEntry(Key key, BlockHandle handle) {}

    private record ParsedBlock(int dataEnd) {}
}
