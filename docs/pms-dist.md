# PMS Linux 发行包

## 1. 定位

`pms-dist` 只负责组装 `pms-server` 的 Linux 二进制发行包，不包含业务实现，也不改变 server
生命周期。MVP 要求目标机器提供 Java 17 或更高版本，不内置 JRE。

发行包采用普通 JAR 与 `lib/*` classpath，不构建 fat JAR。PMS 当前包含 Paimon、Hadoop、
Jetty 和 Log4j 等较多运行时依赖；保留独立 JAR 可以避免合并 service descriptor 和重复资源，
也方便直接检查最终依赖集合。

## 2. 构建与目录

执行：

```bash
mvn -pl pms-dist -am package
```

生成展开目录和内容相同的 `tar.gz`：

```text
pms-dist/target/pms-<version>/
pms-dist/target/pms-<version>.tar.gz
```

包内结构：

```text
pms-<version>/
├── bin/
│   ├── pms-server.sh
│   └── pms-daemon.sh
├── conf/
│   ├── pms-server.properties
│   ├── pms-env.sh
│   └── log4j2.xml
├── lib/
│   └── *.jar
└── README.md
```

`pms-dist` 通过 Maven Assembly Plugin 收集 `pms-server` 的 runtime dependency closure。
Shell 脚本在产物中使用 `0755`，配置文件使用 `0644`。

## 3. 脚本

`bin/pms-server.sh [properties]` 在前台运行 server，并通过 `exec` 让 JVM 直接接收进程信号。
脚本检查 Java 主版本、配置文件、日志配置和 server JAR，然后执行
`org.qwh.pms.server.PmsServerMain`。

`bin/pms-daemon.sh {start|stop|status|restart} [properties]` 提供简单的单实例后台管理：

- `start` 使用 `nohup` 启动，创建 PID 文件并把 stdout/stderr 追加到日志文件。
- `status` 通过 PID 和 JVM 命令行判断当前发行目录的 server 是否仍在运行。
- `stop` 发送 SIGTERM，并最多等待 `PMS_STOP_TIMEOUT` 秒。
- 等待超时只返回失败，不自动发送 SIGKILL。

脚本不调用 `/flush` 或 `/sink`。停机继续遵守 server 的 recovery-first 语义；是否在停机前
主动建立 Paimon fence 属于部署方显式操作，而不是进程脚本的隐式副作用。

## 4. 配置与运行目录

`conf/pms-server.properties` 是 PMS 业务配置。配置仍然只在启动时加载，不支持热更新。
发行包复用 `pms-server-example.properties` 作为初始文件；其中 `/tmp` 路径只便于本地试运行，
生产部署必须改为持久目录。

`conf/pms-env.sh` 是 Shell 进程配置，支持：

| 变量 | 默认值 | 含义 |
|------|--------|------|
| `JAVA_HOME` | 使用 `PATH` | Java 安装目录 |
| `JAVA_OPTS` | 空 | 通用 JVM 参数，兼容已有部署习惯 |
| `PMS_JAVA_OPTS` | 空 | PMS 专用 JVM 参数 |
| `PMS_LOG_LEVEL` | `INFO` | 根日志级别 |
| `PMS_LOG_DIR` | `<PMS_HOME>/logs` | daemon stdout/stderr 目录 |
| `PMS_PID_DIR` | `<PMS_HOME>/run` | PID 文件目录 |
| `PMS_STOP_TIMEOUT` | `120` | SIGTERM 后等待秒数 |

还可以在启动环境中覆盖 `PMS_CONF_DIR`、`PMS_ENV_FILE`、`PMS_LOG_CONFIG`、
`PMS_LOG_FILE` 和 `PMS_PID_FILE`，但 MVP 不为这些变量增加额外配置层。

WAL、storage 和 lookup cache 不属于安装文件。升级发行包时必须继续使用与表 Schema 对应的
原有持久状态目录；Schema 变更则遵守 server 设计，使用全新的本地状态目录。

## 5. MVP 边界

当前发行功能不包含 systemd unit、容器镜像、JRE、安装器、在线升级、日志轮转和自动建表。
日志默认由 Log4j2 输出到控制台，daemon 将其追加到 `pms-server.out`；生产环境应由部署系统
或 logrotate 管理文件轮转。
