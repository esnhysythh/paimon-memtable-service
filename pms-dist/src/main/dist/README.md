# PMS binary distribution

This directory contains the Linux distribution of Paimon MemTable Service.
Java 17 or newer is required.

Before starting PMS, edit `conf/pms-server.properties`. In particular, configure
the bound Paimon table and use durable, non-temporary WAL and storage paths in
production.

Run in the foreground:

```sh
bin/pms-server.sh
```

Run as a background process:

```sh
bin/pms-daemon.sh start
bin/pms-daemon.sh status
bin/pms-daemon.sh stop
```

Both start commands accept an optional properties file path. JVM and process
settings can be changed in `conf/pms-env.sh`.

`stop` sends SIGTERM and waits for the server shutdown hook. PMS uses
recovery-first shutdown: it does not force a final flush or sink while stopping.
The background script does not send SIGKILL when the configured timeout expires.
