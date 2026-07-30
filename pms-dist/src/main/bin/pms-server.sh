#!/bin/sh

set -eu

PMS_BIN_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
PMS_HOME=$(CDPATH= cd -- "$PMS_BIN_DIR/.." && pwd -P)
PMS_ENV_FILE=${PMS_ENV_FILE:-"$PMS_HOME/conf/pms-env.sh"}

if [ -f "$PMS_ENV_FILE" ]; then
    # shellcheck source=/dev/null
    . "$PMS_ENV_FILE"
fi

PMS_CONF_DIR=${PMS_CONF_DIR:-"$PMS_HOME/conf"}
PMS_CONFIG_FILE=${1:-"$PMS_CONF_DIR/pms-server.properties"}
PMS_LOG_CONFIG=${PMS_LOG_CONFIG:-"$PMS_CONF_DIR/log4j2.xml"}
PMS_LOG_LEVEL=${PMS_LOG_LEVEL:-INFO}
PMS_JAVA_OPTS=${PMS_JAVA_OPTS:-}
JAVA_OPTS=${JAVA_OPTS:-}

if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=$(command -v java || true)
fi

if [ -z "$JAVA" ] || [ ! -x "$JAVA" ]; then
    echo "Java was not found. Set JAVA_HOME or add java to PATH." >&2
    exit 1
fi

JAVA_SPEC_VERSION=$(
    "$JAVA" -XshowSettings:properties -version 2>&1 |
        awk -F'= ' '/java.specification.version =/ { print $2; exit }'
)
case "$JAVA_SPEC_VERSION" in
    1.*) JAVA_MAJOR_VERSION=${JAVA_SPEC_VERSION#1.} ;;
    *) JAVA_MAJOR_VERSION=${JAVA_SPEC_VERSION%%.*} ;;
esac
case "$JAVA_MAJOR_VERSION" in
    ''|*[!0-9]*)
        echo "Could not determine Java version from $JAVA." >&2
        exit 1
        ;;
esac
if [ "$JAVA_MAJOR_VERSION" -lt 17 ]; then
    echo "PMS requires Java 17 or newer; found Java $JAVA_SPEC_VERSION." >&2
    exit 1
fi

if [ ! -r "$PMS_CONFIG_FILE" ]; then
    echo "PMS config file is not readable: $PMS_CONFIG_FILE" >&2
    exit 1
fi
if [ ! -r "$PMS_LOG_CONFIG" ]; then
    echo "Log4j2 config file is not readable: $PMS_LOG_CONFIG" >&2
    exit 1
fi

set -- "$PMS_HOME"/lib/pms-server-*.jar
if [ ! -f "$1" ]; then
    echo "pms-server JAR was not found under $PMS_HOME/lib." >&2
    exit 1
fi

# JAVA_OPTS and PMS_JAVA_OPTS are intentionally word-split as JVM argument lists.
# shellcheck disable=SC2086
exec "$JAVA" $JAVA_OPTS $PMS_JAVA_OPTS \
    "-Dlog4j.configurationFile=$PMS_LOG_CONFIG" \
    "-Dpms.log.level=$PMS_LOG_LEVEL" \
    -cp "$PMS_CONF_DIR:$PMS_HOME/lib/*" \
    org.qwh.pms.server.PmsServerMain \
    "$PMS_CONFIG_FILE"
