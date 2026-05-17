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
#
# Starts a single HerdDB Indexing Service locally, on this host, in PUSH mode
# (indexing.log.type=push) -- testing only.
#
# In push mode the indexing service does NOT tail a commit log: a client
# (e.g. VectorBench with --protocol grpc) pushes commit-log entries straight
# in over the PushEntries gRPC RPC. No HerdDB server, no BookKeeper and no
# ZooKeeper are started or needed -- this is the cheapest way to exercise the
# indexing service.
#
# Once it is up (gRPC on localhost:9850), drive ingestion with:
#   vector-testings/run.sh --protocol grpc --grpc-endpoint localhost:9850 \
#       --dataset sift1m --rows 100000
#
# Heap can be overridden with the ISHEAP env var (default 4g).

set -x

BASEDIR=${HERDDB_TESTS_HOME:-target}
ISDIR=$(realpath $BASEDIR/indexing-push)
ZIP=$(ls target/herddb-service*zip)

echo "Installing $ZIP"
rm -Rf $ISDIR

echo "Unzipping Indexing Service in $ISDIR"
TMPUNZIP=$(mktemp -d)
unzip -q $ZIP -d $TMPUNZIP
mv $TMPUNZIP/herddb* $ISDIR
rmdir $TMPUNZIP

cd $ISDIR
mkdir -p tmp

# Configure the indexing service for standalone PUSH mode. A fresh unzip means
# these lines are appended exactly once per run.
INDEXING_CONFIGFILE=conf/indexingservice.properties
echo ""                                    >> $INDEXING_CONFIGFILE
echo "# --- push mode (test-start-indexing-service-push.sh) ---" >> $INDEXING_CONFIGFILE
echo "indexing.log.type=push"              >> $INDEXING_CONFIGFILE
echo "indexing.storage.type=file"          >> $INDEXING_CONFIGFILE
echo "indexing.data.dir=dbdata/indexdata"  >> $INDEXING_CONFIGFILE
echo "server.mode=standalone"              >> $INDEXING_CONFIGFILE

export JAVA_OPTS="-XX:+UseG1GC -Duser.language=en -Djava.net.preferIPv4Stack=true -Xmx${ISHEAP:-4g} -Xms${ISHEAP:-4g} -Djava.awt.headless=true -Djava.util.logging.config.file=conf/logging.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$ISDIR/indexingservice-heapdump.hprof --add-modules jdk.incubator.vector -Djava.io.tmpdir=$(pwd)/tmp"
bin/service indexing-service start

sleep 2

echo "Indexing Service started in PUSH mode (gRPC on localhost:9850)."
echo "No HerdDB server / BookKeeper / ZooKeeper is required."
echo "Drive ingestion with:"
echo "  vector-testings/run.sh --protocol grpc --grpc-endpoint localhost:9850 --dataset sift1m --rows 100000"
