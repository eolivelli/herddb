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

BASEDIR=${HERDDB_TESTS_HOME:-target}
FILESERVERDIR=$(realpath $BASEDIR/fileserver)
SERVER1DIR=$(realpath $BASEDIR/server1)
ZIP=$(ls target/herddb-service*zip)

echo "Installing $ZIP"
rm -Rf $FILESERVERDIR
rm -Rf $SERVER1DIR

echo "Unzipping FileServer in $FILESERVERDIR"
TMPUNZIP=$(mktemp -d)
unzip -q $ZIP -d $TMPUNZIP
mv $TMPUNZIP/herddb* $FILESERVERDIR
rmdir $TMPUNZIP

echo "Unzipping Server in $SERVER1DIR"
TMPUNZIP=$(mktemp -d)
unzip -q $ZIP -d $TMPUNZIP
mv $TMPUNZIP/herddb* $SERVER1DIR
rmdir $TMPUNZIP

# Start the file server (1GB heap)
cd $FILESERVERDIR
mkdir -p tmp
export JAVA_OPTS="-XX:+UseG1GC -Dio.netty.maxDirectMemory=0 -Duser.language=en -Xmx1g -Xms1g -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$FILESERVERDIR/fileserver-heapdump.hprof -Djava.io.tmpdir=$(pwd)/tmp"
bin/service file-server start

sleep 1

# Start HerdDB server with remote file storage (6GB heap)
cd $SERVER1DIR
mkdir -p tmp
CONFIGFILE=conf/server.properties
echo "server.storage.mode=remote" >> $CONFIGFILE
echo "remote.file.servers=localhost:9846" >> $CONFIGFILE
sed -i 's/#http.enable=true/http.enable=true/g' $CONFIGFILE
sed -i 's/server.halt.on.tablespace.boot.error=false/server.halt.on.tablespace.boot.error=false/g' $CONFIGFILE
echo "indexing.service.servers=localhost:9850" >> $CONFIGFILE
export JAVA_OPTS="-XX:+UseG1GC -Djdk.attach.allowAttachSelf=true -Dherddb.vectorindex.rebuild.threads=24 -Duser.language=en -Xmx30g -Xms30g -Dio.netty.maxDirectMemory=0 -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g -XX:+DisableExplicitGC -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SERVER1DIR/server-heapdump.hprof -Djava.io.tmpdir=$(pwd)/tmp"
bin/service server start

sleep 1

# Start the indexing service with remote file storage
cd $SERVER1DIR
INDEXING_CONFIGFILE=conf/indexingservice.properties
echo "log.dir=dbdata/txlog" >> $INDEXING_CONFIGFILE
echo "server.metadata.dir=dbdata/metadata" >> $INDEXING_CONFIGFILE
echo "remote.file.servers=localhost:9846" >> $INDEXING_CONFIGFILE
export JAVA_OPTS="-XX:+UseG1GC -Duser.language=en -Dherddb.vectorindex.rebuild.threads=8 -Djava.net.preferIPv4Stack=true -Xmx40g -Xms40g -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SERVER1DIR/indexingservice-heapdump.hprof --add-modules jdk.incubator.vector -Djava.io.tmpdir=$(pwd)/tmp"
bin/service indexing-service start

sleep 1

# test query
$SERVER1DIR/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from sysnodes'
$SERVER1DIR/bin/herddb-cli.sh -x jdbc:herddb:server:localhost -q 'select * from systablespaces'
