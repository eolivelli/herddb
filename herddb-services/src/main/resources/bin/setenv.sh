# Basic Environment and Java variables
# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#JAVA_HOME=
# Mandatory JVM flags for jvector: the Panama Vector API module and the
# HerdDB-shipped compile-command file that force-inlines jvector's hot paths.
# Appended unconditionally to JAVA_OPTS so they survive a custom JAVA_OPTS
# override (e.g. the Helm chart's server.javaOpts) and so they appear on the
# actual java command line for service processes that pass JAVA_OPTS through
# (server, indexing-service, file-server, bookkeeper). Tools that don't pass
# JAVA_OPTS through (herddb-cli.sh, herddb-bench.sh) intentionally don't get
# them — the CLI doesn't load jvector and the extra startup noise can interfere
# with output capture in some test/exec environments.
JVECTOR_JAVA_OPTS="--add-modules jdk.incubator.vector -XX:CompileCommandFile=conf/jvector-compiler-directives"
# JAVA_OPTS / JDK_JAVA_OPTIONS: when set by the caller, REPLACE the defaults.
# JAVA_OPTS_EXTRA / JDK_JAVA_OPTIONS_EXTRA: appended to the final value, so
# deployments (e.g. the Helm chart's server.javaOptsExtra) can ADD flags
# on top of the defaults without having to re-specify the baseline.
JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:---add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED --enable-native-access=ALL-UNNAMED} ${JDK_JAVA_OPTIONS_EXTRA:-}"
JAVA_OPTS="${JAVA_OPTS:--XX:+UseG1GC -Duser.language=en -Xmx4g -Xms4g -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g -Dio.netty.maxDirectMemory=0 -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties} $JVECTOR_JAVA_OPTS ${JAVA_OPTS_EXTRA:-}"
# Export so the settings reach child java processes started by launcher
# scripts that rely on the JDK picking JDK_JAVA_OPTIONS up automatically
# (e.g. indexing-admin.sh, herddb-cli.sh, vector-bench.sh, bookkeeper).
export JDK_JAVA_OPTIONS JAVA_OPTS

if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=`which java 2>/dev/null`
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_BIN=`dirname $JAVA_PATH 2>/dev/null`
    JAVA_HOME=`dirname $JAVA_BIN 2>/dev/null`
  fi
  if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME environment variable is not defined and is needed to run this program"
    exit 1
  fi
fi
