#!/bin/sh

# Optional Java installation. When unset, PMS uses java from PATH.
# JAVA_HOME=/usr/lib/jvm/java-17

# JVM options for PMS. JAVA_OPTS is also honored for compatibility.
# PMS_JAVA_OPTS="-Xms1g -Xmx4g"

# Runtime directories. The defaults make an unpacked distribution directly
# usable; production deployments may place them outside PMS_HOME.
# PMS_LOG_DIR=/var/log/pms
# PMS_PID_DIR=/run/pms

# Logging and shutdown controls.
PMS_LOG_LEVEL=${PMS_LOG_LEVEL:-INFO}
PMS_STOP_TIMEOUT=${PMS_STOP_TIMEOUT:-120}
