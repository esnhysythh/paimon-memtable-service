package org.qwh.pms.core.sink;

public record SinkFileRef(
    String fileName,
    String path,
    long fileSize,
    long rowCount,
    String partition,
    int bucket
) {}
