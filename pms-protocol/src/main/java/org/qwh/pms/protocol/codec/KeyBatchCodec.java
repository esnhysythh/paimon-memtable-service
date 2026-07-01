package org.qwh.pms.protocol.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class KeyBatchCodec {

    private KeyBatchCodec() {}

    public static byte[] encodeRequest(List<byte[]> keys) {
        Objects.requireNonNull(keys, "keys must not be null");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("key batch must not be empty");
        }
        BinaryPayloadWriter writer = new BinaryPayloadWriter();
        VarInt.writeUnsignedInt(writer, keys.size());
        for (byte[] key : keys) {
            Objects.requireNonNull(key, "key must not be null");
            VarInt.writeUnsignedInt(writer, key.length);
            writer.writeBytes(key);
        }
        return writer.toByteArray();
    }

    public static byte[] encodeSingle(byte[] key) {
        return encodeRequest(List.of(key));
    }

    public static List<byte[]> decodeRequest(byte[] body) {
        return decodeRequest(body, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static List<byte[]> decodeRequest(byte[] body, int maxKeys, int maxKeyBytes) {
        requirePositive(maxKeys, "maxKeys");
        requirePositive(maxKeyBytes, "maxKeyBytes");
        BinaryPayloadReader reader = new BinaryPayloadReader(body);
        int count = VarInt.readUnsignedInt(reader);
        if (count == 0) {
            throw new IllegalArgumentException("key count must be positive");
        }
        if (count > maxKeys) {
            throw new IllegalArgumentException("key count exceeds limit: " + count + " > " + maxKeys);
        }
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyLength = VarInt.readUnsignedInt(reader);
            if (keyLength > maxKeyBytes) {
                throw new IllegalArgumentException("key too large: " + keyLength + " > " + maxKeyBytes);
            }
            keys.add(reader.readBytes(keyLength));
        }
        reader.requireFullyRead();
        return List.copyOf(keys);
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
