package org.qwh.pms.protocol.api;

import java.util.List;

public final class PmsProtocolConstants {

    public static final int PROTOCOL_VERSION = 1;
    public static final String PROTOCOL_NAME = "pms-http2-binary";
    public static final String REQUIRED_HTTP_VERSION = "HTTP_2";
    public static final String CONTENT_TYPE_BINARY = "application/octet-stream";

    public static final String API_PREFIX = "/pms/api/v1";
    public static final String HANDSHAKE_PATH = API_PREFIX + "/handshake";
    public static final String LOCAL_PUT_PATH = API_PREFIX + "/local/put";
    public static final String LOCAL_DELETE_PATH = API_PREFIX + "/local/delete";
    public static final String LOCAL_WRITE_BATCH_PATH = API_PREFIX + "/local/writeBatch";
    public static final String LOCAL_GET_PATH = API_PREFIX + "/local/get";
    public static final String LOCAL_GET_PREFIX_PATH = API_PREFIX + "/local/getPrefix";
    public static final String FULL_GET_PATH = API_PREFIX + "/full/get";

    public static final String CAPABILITY_LOCAL_PUT = "localPut";
    public static final String CAPABILITY_LOCAL_DELETE = "localDelete";
    public static final String CAPABILITY_LOCAL_WRITE_BATCH = "localWriteBatch";
    public static final String CAPABILITY_LOCAL_GET = "localGet";
    public static final String CAPABILITY_LOCAL_GET_PREFIX = "localGetPrefix";
    public static final String CAPABILITY_FULL_GET = "fullGet";
    public static final String CAPABILITY_STRICT_HTTP2 = "strictHttp2";

    public static final List<String> REQUIRED_HOT_PATH_CAPABILITIES = List.of(
            CAPABILITY_LOCAL_PUT,
            CAPABILITY_LOCAL_DELETE,
            CAPABILITY_LOCAL_WRITE_BATCH,
            CAPABILITY_LOCAL_GET,
            CAPABILITY_LOCAL_GET_PREFIX,
            CAPABILITY_FULL_GET);

    private PmsProtocolConstants() {}
}
