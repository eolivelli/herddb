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
# Mandatory JVM flags always appended unconditionally to JAVA_OPTS.
# They survive a custom JAVA_OPTS override (e.g. the Helm chart's
# server.javaOpts / indexingService.javaOpts) and appear on the actual java
# command line for service processes that pass JAVA_OPTS through (server,
# indexing-service, file-server, bookkeeper). Tools that don't pass JAVA_OPTS
# through (herddb-cli.sh, herddb-bench.sh) intentionally don't get them.
#
# jvector: Panama Vector API module + the HerdDB compile-command file that
# force-inlines jvector's SIMD hot paths.
#
# DNS negative-result caching (issue #510 — CoreDNS propagation race):
#   networkaddress.cache.negative.ttl=0  prevents the JVM from caching DNS
#     failures; the JRE default (10 s on most JDKs) causes a 10-second
#     blackout per retry when CoreDNS hasn't yet published the headless-
#     Service A record for a freshly started pod (e.g. the file server).
#     Combined with the existing retry back-off in RemoteFileServiceClient
#     this reduced a ~31 s connectivity blackout to at most one retry cycle.
#   networkaddress.cache.ttl=30 caps the positive DNS cache to 30 s so the
#     JVM tracks pod-IP changes within one TTL window. Without a SecurityManager
#     (the normal HerdDB case) the JDK default is 30 s, so this setting is a
#     no-op for the positive cache — it is stated explicitly for clarity and
#     to lock in the value should the default ever change or a SecurityManager
#     be introduced. With a SecurityManager the JDK default is -1 (cache
#     forever), which would prevent pod-failover recovery without a JVM restart.
JVECTOR_JAVA_OPTS="--add-modules jdk.incubator.vector -XX:CompileCommandFile=conf/jvector-compiler-directives -Dnetworkaddress.cache.negative.ttl=0 -Dnetworkaddress.cache.ttl=30"
# JAVA_OPTS / JDK_JAVA_OPTIONS: when set by the caller, REPLACE the defaults.
# JAVA_OPTS_EXTRA / JDK_JAVA_OPTIONS_EXTRA: appended to the final value, so
# deployments (e.g. the Helm chart's server.javaOptsExtra) can ADD flags
# on top of the defaults without having to re-specify the baseline.
#
# --add-opens=java.base/java.nio=ALL-UNNAMED + -Dio.netty.tryReflectionSetAccessible=true
# are required so that Netty's PlatformDependent can reflectively install its own
# off-heap memory accounting and avoid the JVM's Bits.reserveMemory direct-buffer
# limit on JDK 9+. Without these flags Netty falls back to ByteBuffer.allocateDirect
# and every direct allocation (pooled and unpooled) is bounded by -XX:MaxDirectMemorySize,
# which causes OutOfMemoryError under heavy ingestion.
#
# --sun-misc-unsafe-memory-access=allow re-enables sun.misc.Unsafe memory-access
# methods on JDK 24+ (where the default is "warn" and may become "deny" on
# JDK 26+). Netty 4.1.120 and 4.1.121 disabled their internal Unsafe usage by
# default to silence the JEP 498 warnings, which made
# PlatformDependent.useDirectBufferNoCleaner() return false and forced every
# direct allocation back through Bits.reserveMemory — defeating
# -Dio.netty.maxDirectMemory.  This flag restores the no-cleaner pooled path
# until we upgrade to Netty 4.1.122+ (where the default was reverted) or 4.2.x
# (FFM-based and Unsafe-free).
JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:---add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dio.netty.tryReflectionSetAccessible=true} ${JDK_JAVA_OPTIONS_EXTRA:-}"
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
