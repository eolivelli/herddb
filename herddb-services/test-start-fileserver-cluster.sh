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

FILESERVERDIR=$(realpath target/fileserver)
SERVER1DIR=$(realpath target/server1)
ZIP=$(ls target/herddb-service*zip)

echo "Installing $ZIP"
rm -Rf $FILESERVERDIR
rm -Rf $SERVER1DIR
mkdir $FILESERVERDIR
mkdir $SERVER1DIR

echo "Unzipping FileServer in $FILESERVERDIR"
unzip -q $ZIP -d $FILESERVERDIR
unzip -q $ZIP -d $SERVER1DIR

# Start the file server (1GB heap)
cd $FILESERVERDIR/herddb*
export JAVA_OPTS="-XX:+UseG1GC -Dio.netty.maxDirectMemory=0 -Duser.language=en -Xmx1g -Xms1g -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$FILESERVERDIR/fileserver-heapdump.hprof"
bin/service file-server start
cd ../..

sleep 1

# Start HerdDB server in remote-file-service mode (6GB heap)
cd $SERVER1DIR/herddb*
CONFIGFILE=conf/server.properties
sed -i 's/server.mode=standalone/server.mode=remote-file-service/g' $CONFIGFILE
sed -i 's/#http.enable=true/http.enable=false/g' $CONFIGFILE
sed -i 's/server.halt.on.tablespace.boot.error=true/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
export JAVA_OPTS="-XX:+UseG1GC -Duser.language=en -Xmx7g -Xms7g -Dio.netty.maxDirectMemory=0 -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SERVER1DIR/server-heapdump.hprof"
bin/service server start
cd ../..

sleep 1

# test query
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from sysnodes'
$SERVER1DIR/herddb*/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from systablespaces'
