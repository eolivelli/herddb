/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.log.IndexingServiceRebalanceDescriptor;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * JOINING-fallback bootstrap of the indexing-service engine.
 *
 * <p>An engine started with
 * {@link IndexingServerConfiguration#PROPERTY_BOOTSTRAP_FROM_REBALANCE}=true
 * boots in {@link IndexingServiceEngine.EngineStatus#JOINING}: it drops
 * every commit-log entry until it sees an {@code INDEXING_SERVICE_REBALANCE}
 * entry. That entry installs the schema (tables + vector indexes) and
 * transitions the engine to {@code ACTIVE}, after which subsequent log
 * entries are applied normally.
 *
 * @author enrico.olivelli
 */
public class JoiningFallbackTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int VECTOR_DIM = 3;

    private Table table() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index vectorIndex(int numShards, int numInstances) {
        return Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .property(VectorIndexManager.PROP_NUM_INSTANCES, String.valueOf(numInstances))
                .build();
    }

    private IndexingServerConfiguration joiningConfig(int instanceId, int numInstances) {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstances));
        props.setProperty(IndexingServerConfiguration.PROPERTY_BOOTSTRAP_FROM_REBALANCE,
                "true");
        return new IndexingServerConfiguration(props);
    }

    /**
     * The full lifecycle: pod boots in JOINING, drops a synthetic stream of
     * pre-REBALANCE DDL+DML, transitions to ACTIVE on a REBALANCE that
     * carries the schema, then applies post-REBALANCE inserts normally.
     */
    @Test
    public void bootsJoiningWaitsForRebalanceThenActivates() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, joiningConfig(0, 1))) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertEquals(IndexingServiceEngine.EngineStatus.JOINING, engine.getEngineStatus());

            Table t = table();
            Index ix = vectorIndex(2, 1);

            // Pre-REBALANCE entries: dropped because engine is JOINING
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            for (int i = 0; i < 5; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "lost-" + i, "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();

            // Engine is still JOINING and has applied nothing.
            assertEquals(IndexingServiceEngine.EngineStatus.JOINING, engine.getEngineStatus());

            // REBALANCE arrives — engine transitions to ACTIVE
            IndexingServiceRebalanceDescriptor descriptor = new IndexingServiceRebalanceDescriptor(
                    100L, 1,
                    Collections.singletonList(t),
                    Collections.singletonList(ix));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 100),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            engine.awaitPendingWorkForTest();

            assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE, engine.getEngineStatus());
            assertNotNull("descriptor must be recorded after JOINING bootstrap",
                    engine.getLastObservedRebalance());
            assertEquals(100L, engine.getObservedRebalanceEpoch());

            // Pre-REBALANCE inserts must NOT be present
            assertEquals("pre-REBALANCE inserts must be dropped",
                    0, engine.search("default", "mytable", "vidx",
                            new float[]{0, 0, 0}, 100).size());

            // Post-REBALANCE inserts ARE applied
            int post = 7;
            for (int i = 0; i < post; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "kept-" + i, "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 200 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            assertEquals("post-REBALANCE inserts must land in the index",
                    post, engine.search("default", "mytable", "vidx",
                            new float[]{0, 0, 0}, 100).size());
        }
    }

    /**
     * Default config (no bootstrap-from-rebalance) keeps current behaviour:
     * the engine boots in ACTIVE and applies entries from the start.
     */
    @Test
    public void defaultConfigBootsActive() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, 0, 1)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE, engine.getEngineStatus());

            Table t = table();
            Index ix = vectorIndex(2, 1);
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            Record rec = RecordSerializer.makeRecord(t,
                    "pk", "k1", "vec", new float[]{1, 1, 1});
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 3),
                    LogEntryFactory.insert(t, rec.key, rec.value, null));
            engine.awaitPendingWorkForTest();

            assertEquals(1, engine.search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, 100).size());
        }
    }
}
