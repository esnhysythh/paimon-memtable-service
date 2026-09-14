# PMS 本地 Benchmark

## 范围

`pms-tests/pms-benchmark` 提供三类本地基准：

| 分类 | 入口 | 测量路径 |
| --- | --- | --- |
| Core | `core.CoreDbBenchMain` | byte-oriented core 插入、MemTable / 本地 SST 查询 |
| Paimon direct | `paimon.PaimonLookupBenchMain --mode=direct` | 真实本地 Paimon 文件，经生产 lookup 入口直读 Parquet |
| Paimon cached | `paimon.PaimonLookupBenchMain --mode=cached` | 同样的 Paimon 查询，经生产 router 命中本地 Value SST |

模块依赖 `pms-core` 和 `pms-lookup-paimon`；core case 不调用 Paimon，Paimon case 不调用 core。
不启动 server、不经过 HTTP、不连接 HDFS/S3。core 的 `sinked` 使用 fake sink，不能当作真实
Paimon sink 性能。Paimon case 的写入/commit 仅用于准备数据，不测写入性能。

源码仓库维护 runner、正确性测试、执行脚本和本说明；运行生成的 CSV、日志、manifest 与报告
保留本地，推荐输出到模块 `target/`。`reports/` 中的历史实验资料也仅保留本地。

## Core：构建与运行

```bash
export JAVA_HOME=/path/to/java17
mvn -o -pl pms-tests/pms-benchmark -am verify
java -Xms1g -Xmx1g -jar pms-tests/pms-benchmark/target/pms-benchmark-0.1-SNAPSHOT-runner.jar \
  --benchmarks=fillseq,fillrandom --num=100000 --threads=4 \
  --value_size=100 --warmup_runs=1 --runs=3 --delete_temp_db=true
```

新机器未缓存依赖时，首次构建去掉 `-o`。性能测量使用独立 Java 进程和固定堆参数；普通 Maven
构建只编译模块和运行小规模正确性测试，不自动运行性能矩阵。runner JAR 不进入 PMS 发行包。

本地查询示例：

```bash
java -Xms1g -Xmx1g -jar pms-tests/pms-benchmark/target/pms-benchmark-0.1-SNAPSHOT-runner.jar \
  --benchmarks=readrandom,readmissing --num=100000 --reads=1000000 \
  --prepare=sst --threads=4 --warmup_runs=1 --runs=3 --delete_temp_db=true
```

将 `--prepare` 改为 `memtable` 可测内存层。固定矩阵脚本使用 100B/1KiB value、1/4 线程，分别运行
插入、MemTable 和 SST 查询，每个配置独立 JVM；保存原始 CSV、stderr 配置、JVM 参数、Git 状态、
runner SHA-256、完整命令和汇总。Python 仅用标准库：

```bash
python3 pms-tests/pms-benchmark/scripts/run_local.py \
  --java-home "$JAVA_HOME" --output /absolute/path/to/new-benchmark-report
```

输出目录必须不存在。可用 `--num`、`--reads`、`--runs` 调整矩阵规模。

## 测量口径

- `num` 是插入/预加载条数；`reads` 是每轮查询操作数，默认等于 num，矩阵默认 100 万次。
- 默认 1 轮预热、3 轮测量。每个 case/iteration 使用新的本地状态；warmup 行保留但不纳入汇总。
- 查询在计时前预加载到指定层并验证层状态，不能将已自动 freeze 的数据标成 MemTable 基准。
  每次 HIT/MISS 均断言预期状态，计时外抽样核对完整 value；写入结束也抽样核对插入值。
- key 至少 8 字节；随机插入使用确定性的 64 位置换产生唯一 key。顺序插入在多线程下按逻辑
  顺序分块调度，不保证实际提交顺序完全递增。
- key/value 构造、core 调用及写入时遇到 immutable 的同步 Flush 都计入测量；预加载、
  正确性抽样、最终 close、目录大小统计和清理不计时。不在 case 结束时强制 flush 剩余 MemTable。
- 当前 WAL append 使用 `force=false`。结果包含 WAL 序列化与本地文件写入，**不是逐条 fsync
  的持久写吞吐**；本轮不改变该生产语义，也不验证断电场景。
- SST 在本轮进程中刚生成，OS page cache 可命中；没有清空系统缓存，结果不是冷盘随机读。
- 默认最多采样 10,000 次操作；p99/max 是采样值。汇总报告采用各测量轮吞吐的中位数和范围，
  p99 采用各轮采样 p99 的中位数，不能解释为合并全部请求的 p99。
- 内存查询、BloomFilter MISS 路径很短，任务分块、采样和线程调度开销会影响多线程扩展性。
  数值是当前机器上的观察，不设通用 SLA，不推算远端或 HTTP 性能。

## Case 与参数

| Case | 说明 |
| --- | --- |
| fillseq / fillrandom | 顺序逻辑 key / 唯一随机 key 插入 |
| readrandom / readmissing | 已存在 key 的随机 HIT / 范围内奇数 key 的 MISS（预加载偶数 key） |
| overwrite / deleterandom | 预加载后随机覆盖或删除，抽取 key 可重复 |
| readwhilewriting | 按操作数 1:1 读取预加载 key 与写入新 key |
| flush / compact / recover | 保留的维护诊断 case；延迟单位分别是 flush/compact/init，ops 按记录数折算，不与单条 put/get 延迟比较 |

`--prepare=memtable|immutable|sst|sinked` 控制预加载层；容量阈值由
`--memtable_max_entries`、`--memtable_max_size_mb` 控制。`--seed` 默认 24301，
`--key_size` 默认 16，`--value_size` 默认 100，`--use_mmap` 默认 false。

stdout 是带 phase/iteration 的 CSV；stderr 输出数据目录与完整有效参数、JVM/OS 信息。
`--db` 现在只表示运行父目录，程序总是创建独占的新子目录；不再接受会删除或混用已有状态的
`--fresh` / `--reuse_db`。`--delete_temp_db=true` 删除本次成功 iteration；失败现场保留。

## Paimon direct / cached

先执行前面的 Maven 构建命令。保留现有 core runner 的默认入口，Paimon 通过 classpath 指定入口：

```bash
java -Xms1g -Xmx1g -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -cp pms-tests/pms-benchmark/target/pms-benchmark-0.1-SNAPSHOT-runner.jar \
  org.qwh.pms.benchmark.paimon.PaimonLookupBenchMain \
  --mode=direct --num=100000 --reads=20000 --threads=1 \
  --value_size=100 --warmup_runs=1 --runs=3
```

将 `--mode` 改成 `cached` 即测缓存路径。默认参数如上，seed=24301；`--db` 是运行父目录，
每次在其下创建独占子目录，默认位于模块 `target/paimon-data`。成功后自动清理；
`--delete_temp_db=false` 可保留本次数据，失败现场总是保留，不删除已有父目录内容。

运行 1/4 线程 × direct/cached 的对照矩阵，每个配置使用独立 JVM：

```bash
python3 pms-tests/pms-benchmark/scripts/run_paimon.py \
  --java-home "$JAVA_HOME" \
  --output pms-tests/pms-benchmark/target/paimon-results
```

输出目录必须不存在。默认 10 万行、100B payload；direct 每轮 2 万次查询，cached 每轮 100 万次，
各预热 1 轮、正式测量 3 轮。cached 更快，增加操作数用于避免测量时间过短。
两者使用相同 seed 和生成规则，较短序列是较长序列的前缀；操作数不同时不是逐轮完全相同的请求集。
可用 `--reads`、`--cached-reads` 设置相同操作数做严格序列对照；另有 `--num`、`--value-size`、`--runs`。

### 固定场景与正确性

- 使用真实 Paimon primary-key 表：无分区、单 bucket、`INT id + STRING payload`，保存偶数 key。
  payload 是按 key 确定生成的 ASCII 字符串，参数表示 payload 字节数，不包含 id 和 Paimon 内部字段。
- 一次 batch commit 后从 committed snapshot 读取 live files。首版固定单 data file；数据量过大
  生成多个文件时明确失败，不悄悄改变测量布局。两种模式的数据内容和生成选项相同。
- HIT 查询随机偶数 key；MISS 查询相邻偶数之间的奇数 key，全部处于文件 key range 内，避免
  只测到文件范围排除。文件内部的 row-group/page 筛选与 BloomFilter 仍按生产实现执行。
- direct 直接接入生产 `PaimonKeyValueParquetLookup`，不创建 Value SST router/builder。
  cached 使用生产 `ThresholdFileLookupRouter` 和 `ValueSstCacheBuilder`；准备阶段以同步 executor
  完成构建，确认 READY 后才测量。测量阶段两者均经过 `PaimonKeyValueLookupService` 的候选规划。
- 计时前最多抽样 64 个 key 检查完整值和 MISS；计时内每次检查 HIT/MISS 状态。
  每轮要求 direct 恰有 operations 次 direct 查询，cached 恰有 operations 次 local 查询且无 direct
  回退。任何错误或路由不符立即失败、非零退出；失败轮不输出有效性能行。

### 指标与解释

CSV 记录吞吐、采样 p99、错误数（成功轮为 0），以及证明路径的 direct/cached 计数。
脚本汇总正式轮吞吐的中位数和范围，以及各轮 p99 的中位数。后者不是所有请求合并后的 p99。
每轮最多约 1 万个延迟样本；任务静态分给线程，没有每次查询竞争的任务分配器。
吞吐计时包含 lookup 调用、结果状态检查和采样开销；p99 采样只包围 lookup 调用。

数据准备、commit、snapshot 安装、cache build、完整值抽样、线程池创建和最终清理均不计入查询耗时。
轮间复用查询服务和缓存；metadata、Value SST reader 的内存缓存及 OS page cache 可以命中。
这是一组热态本地基线，不能解释为冷盘性能、远端性能或 HTTP 端到端性能。

cached 测的是文件物化后的 Value SST 查询，也可能命中其 reader 的内存缓存，不保证每次都读磁盘。
当前 `ValueSstCacheEntry.lookup` 按文件同步，单文件场景不保证多线程扩展；保留此生产行为，
不为 benchmark 创建独立每线程缓存副本。direct 保留全部生产 Parquet 筛选、定位和解码成本。

为避免每次读取的日志 IO 干扰测量，Paimon CLI 默认将 SLF4J/JUL INFO 日志关闭，保留 WARNING/ERROR。
输出包含配置、Java/OS、文件大小与缓存大小；脚本另存完整命令、JAR 哈希和 Git 状态。

## 后续

优先补充更大工作集、多 SST 和持续 Flush 下的插入/查询，以及有正确性对照的混合读写。
需要 server-local 性能时，再增加真实 client + 本地 warehouse 的独立场景，明确区分 HTTP 开销。
JMH 微基准、远端 workload、持久化模式对照和正式性能回归阈值单独安排。
