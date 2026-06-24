package org.qwh.pms.lookup.key;

import org.apache.paimon.types.DataType;

import java.util.List;

/** Registry for key types supported by direct Parquet lookup. */
public final class KeyTypeCodecs {

    private static final List<KeyTypeCodec> CODECS =
            List.of(
                    new NumericKeyTypeCodec(),
                    new StringKeyTypeCodec(),
                    new TimestampInt64KeyTypeCodec());

    private KeyTypeCodecs() {}

    public static boolean isSupported(DataType type) {
        return CODECS.stream().anyMatch(codec -> codec.supports(type));
    }
}
