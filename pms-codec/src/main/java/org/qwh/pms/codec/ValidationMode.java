package org.qwh.pms.codec;

/**
 * Controls how much structural validation is performed while parsing row-value metadata.
 *
 * <p>{@link #TRUSTED} is the default for PMS internal reads. It keeps boundary checks but skips
 * sorted/disjoint field-id checks, assuming bytes were produced by the current codec. Use {@link
 * #STRICT} for tests, debugging, historical data, or external KV boundaries.
 */
public enum ValidationMode {
    STRICT,
    TRUSTED
}
