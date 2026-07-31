package org.qwh.pms.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

/** PMS Flink Connector 的 SQL 配置项. */
public final class PmsConnectorOptions {

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PMS Server 根 URI.");

    public static final ConfigOption<Duration> CLIENT_CONNECT_TIMEOUT =
            ConfigOptions.key("client.connect-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("PMS handshake 和连接超时.");

    public static final ConfigOption<Duration> CLIENT_WRITE_TIMEOUT =
            ConfigOptions.key("client.write-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("单次 PMS 写请求超时, 0 表示不设置 request timeout.");

    public static final ConfigOption<Duration> CLIENT_READ_TIMEOUT =
            ConfigOptions.key("client.read-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(3))
                    .withDescription("单次 PMS Lookup 请求超时, 0 表示不设置 request timeout.");

    public static final ConfigOption<Boolean> CLIENT_REQUIRE_HTTP2 =
            ConfigOptions.key("client.require-http2")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("是否拒绝非 HTTP/2 响应.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("PMS Sink 并行度.");

    public static final ConfigOption<Integer> SINK_BATCH_MAX_ROWS =
            ConfigOptions.key("sink.batch.max-rows")
                    .intType()
                    .defaultValue(256)
                    .withDescription("单个 PMS 写 batch 的最大记录数.");

    public static final ConfigOption<MemorySize> SINK_BATCH_MAX_BYTES =
            ConfigOptions.key("sink.batch.max-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription("单个 PMS 写 batch 的最大估算编码大小.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(50))
                    .withDescription("非空 Sink buffer 的最大等待时间, 0 表示每条及时 flush.");

    public static final ConfigOption<Integer> SINK_WRITE_RETRY_MAX_RETRIES =
            ConfigOptions.key("sink.write-retry.max-retries")
                    .intType()
                    .defaultValue(10)
                    .withDescription("明确拒绝写状态的最大重试次数.");

    public static final ConfigOption<Duration> SINK_WRITE_RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.write-retry.initial-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10))
                    .withDescription("Sink 写重试初始退避.");

    public static final ConfigOption<Duration> SINK_WRITE_RETRY_MAX_BACKOFF =
            ConfigOptions.key("sink.write-retry.max-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(640))
                    .withDescription("Sink 写重试最大退避.");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("是否使用异步 Lookup provider.");

    public static final ConfigOption<Integer> LOOKUP_ASYNC_THREAD_NUMBER =
            ConfigOptions.key("lookup.async.thread-number")
                    .intType()
                    .defaultValue(4)
                    .withDescription("每个 async Lookup subtask 的 worker 数.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Lookup 失败后的最大重试次数.");

    public static final ConfigOption<Duration> LOOKUP_RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("lookup.retry.initial-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10))
                    .withDescription("Lookup 重试初始退避.");

    public static final ConfigOption<Duration> LOOKUP_RETRY_MAX_BACKOFF =
            ConfigOptions.key("lookup.retry.max-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(640))
                    .withDescription("Lookup 重试最大退避.");

    private PmsConnectorOptions() {}
}
