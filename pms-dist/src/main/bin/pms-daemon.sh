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
PMS_LOG_DIR=${PMS_LOG_DIR:-"$PMS_HOME/logs"}
PMS_PID_DIR=${PMS_PID_DIR:-"$PMS_HOME/run"}
PMS_LOG_FILE=${PMS_LOG_FILE:-"$PMS_LOG_DIR/pms-server.out"}
PMS_PID_FILE=${PMS_PID_FILE:-"$PMS_PID_DIR/pms-server.pid"}
PMS_STOP_TIMEOUT=${PMS_STOP_TIMEOUT:-120}
PMS_CONFIG_FILE=${2:-"$PMS_CONF_DIR/pms-server.properties"}
PMS_MAIN_CLASS=org.qwh.pms.server.PmsServerMain
PMS_PID=

usage() {
    echo "Usage: $0 {start|stop|status|restart} [pms-server.properties]" >&2
}

read_pid() {
    [ -r "$PMS_PID_FILE" ] || return 1
    PMS_PID=$(sed -n '1p' "$PMS_PID_FILE")
    case "$PMS_PID" in
        ''|*[!0-9]*) return 1 ;;
    esac
}

is_pms_process() {
    pid=$1
    kill -0 "$pid" 2>/dev/null || return 1
    args=$(ps -p "$pid" -o args= 2>/dev/null || true)
    case "$args" in
        *"$PMS_HOME"*"$PMS_MAIN_CLASS"*) return 0 ;;
        *) return 1 ;;
    esac
}

start_server() {
    if read_pid; then
        if is_pms_process "$PMS_PID"; then
            echo "PMS is already running with PID $PMS_PID."
            return 1
        fi
        echo "Removing stale PID file: $PMS_PID_FILE"
        rm -f -- "$PMS_PID_FILE"
    fi

    mkdir -p -- "$PMS_LOG_DIR" "$PMS_PID_DIR"
    nohup "$PMS_HOME/bin/pms-server.sh" "$PMS_CONFIG_FILE" \
        </dev/null >>"$PMS_LOG_FILE" 2>&1 &
    PMS_PID=$!

    pid_tmp="$PMS_PID_FILE.$$"
    printf '%s\n' "$PMS_PID" >"$pid_tmp"
    mv -f -- "$pid_tmp" "$PMS_PID_FILE"

    sleep 1
    if is_pms_process "$PMS_PID"; then
        echo "Started PMS with PID $PMS_PID. Log: $PMS_LOG_FILE"
        return 0
    fi

    rm -f -- "$PMS_PID_FILE"
    echo "PMS failed to start. Check $PMS_LOG_FILE." >&2
    return 1
}

stop_server() {
    if ! read_pid; then
        echo "PMS is not running."
        return 0
    fi
    if ! is_pms_process "$PMS_PID"; then
        echo "PMS is not running; removing stale PID file."
        rm -f -- "$PMS_PID_FILE"
        return 0
    fi

    case "$PMS_STOP_TIMEOUT" in
        ''|*[!0-9]*)
            echo "PMS_STOP_TIMEOUT must be a non-negative integer." >&2
            return 1
            ;;
    esac

    echo "Stopping PMS with PID $PMS_PID..."
    kill "$PMS_PID"
    elapsed=0
    while is_pms_process "$PMS_PID"; do
        if [ "$elapsed" -ge "$PMS_STOP_TIMEOUT" ]; then
            echo "PMS did not stop within ${PMS_STOP_TIMEOUT}s; PID $PMS_PID is still running." >&2
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    rm -f -- "$PMS_PID_FILE"
    echo "PMS stopped."
}

status_server() {
    if read_pid && is_pms_process "$PMS_PID"; then
        echo "PMS is running with PID $PMS_PID."
        return 0
    fi
    echo "PMS is not running."
    return 1
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    usage
    exit 1
fi

case "$1" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    status)
        status_server
        ;;
    restart)
        stop_server
        start_server
        ;;
    *)
        usage
        exit 1
        ;;
esac
