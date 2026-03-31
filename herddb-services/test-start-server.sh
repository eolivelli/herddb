#/bin/bash
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

set -x

SERVER1DIR=$(realpath target/server1)
ZIP=$(ls target/herddb-service*zip)

echo "Installing $ZIP"
rm -Rf $SERVER1DIR
mkdir $SERVER1DIR

echo "Unzipping Server in $SERVER1DIR"
unzip -q $ZIP -d $SERVER1DIR

# Start HerdDB server in standalone mode (6GB heap)
cd $SERVER1DIR/herddb*
mkdir -p tmp
CONFIGFILE=conf/server.properties
sed -i 's/#http.enable=true/http.enable=true/g' $CONFIGFILE
sed -i 's/server.halt.on.tablespace.boot.error=false/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
export JAVA_OPTS="-XX:+UseG1GC -Djdk.attach.allowAttachSelf=true -Dherddb.vectorindex.rebuild.threads=24 -Dserver.checkpoint.memory.limit=4294967296 -Dserver.checkpoint.period=300000 -Duser.language=en -Xmx50g -Xms50g -Dio.netty.maxDirectMemory=0 -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SERVER1DIR/server-heapdump.hprof -Djava.io.tmpdir=$(pwd)/tmp"
bin/service server start
cd ../..

sleep 1

# test query
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from sysnodes'
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from systablespaces'
