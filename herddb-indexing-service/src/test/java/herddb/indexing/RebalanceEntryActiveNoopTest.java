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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Active-engine handling of an {@code INDEXING_SERVICE_REBALANCE} entry.
 *
 * <p>Routing is per-index (baked at CREATE INDEX time), so the entry has no
 * effect on routing for an active engine. The handler must still record the
 * descriptor for diagnostics and treat lower-or-equal epochs as no-ops.
 *
 * @author enrico.olivelli
 */
public class RebalanceEntryActiveNoopTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Table table() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index index(int numShards, int numInstances) {
        return Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .property(VectorIndexManager.PROP_NUM_INSTANCES, String.valueOf(numInstances))
                .build();
    }

    @Test
    public void recordsDescriptorAndIsIdempotent() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 2)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertNull(engine.getLastObservedRebalance());

            // Build a descriptor and feed it as a synthetic REBALANCE entry
            Table t = table();
            Index ix = index(4, 2);
            IndexingServiceRebalanceDescriptor d1 = new IndexingServiceRebalanceDescriptor(
                    100L, 4, Collections.singletonList(t), Collections.singletonList(ix));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 1),
                    LogEntryFactory.indexingServiceRebalance(d1));
            engine.awaitPendingWorkForTest();

            assertNotNull(engine.getLastObservedRebalance());
            assertEquals(100L, engine.getObservedRebalanceEpoch());
            assertEquals(4, engine.getLastObservedRebalance().defaultNumInstances);

            // Replay same epoch -> no-op
            IndexingServiceRebalanceDescriptor previous = engine.getLastObservedRebalance();
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 2),
                    LogEntryFactory.indexingServiceRebalance(d1));
            engine.awaitPendingWorkForTest();
            assertSame("same epoch must not replace the recorded descriptor",
                    previous, engine.getLastObservedRebalance());

            // Older epoch -> no-op
            IndexingServiceRebalanceDescriptor d0 = new IndexingServiceRebalanceDescriptor(
                    50L, 8, Collections.emptyList(), Collections.emptyList());
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 3),
                    LogEntryFactory.indexingServiceRebalance(d0));
            engine.awaitPendingWorkForTest();
            assertSame("older epoch must not replace the recorded descriptor",
                    previous, engine.getLastObservedRebalance());

            // Newer epoch -> replaces
            IndexingServiceRebalanceDescriptor d2 = new IndexingServiceRebalanceDescriptor(
                    200L, 8,
                    Arrays.asList(t),
                    Arrays.asList(index(4, 8)));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 4),
                    LogEntryFactory.indexingServiceRebalance(d2));
            engine.awaitPendingWorkForTest();
            assertEquals(200L, engine.getObservedRebalanceEpoch());
            assertEquals(8, engine.getLastObservedRebalance().defaultNumInstances);
        }
    }

    /**
     * Routing must continue to use the per-index PROP_NUM_INSTANCES even
     * after a REBALANCE entry is observed: existing indexes are not
     * re-routed.
     */
    @Test
    public void rebalanceEntryDoesNotChangeRoutingOfExistingIndex() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 1, 4)) {
            // Engine instance 1 of 4 (bootstrap default 4)
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();

            Table t = table();
            // Old index with N=2: instance 1 IS an owner (instanceId 1 < 2)
            Index oldIx = index(4, 2);
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(oldIx, null));

            // Insert until at least one record routes to instance 1
            int numRecords = 200;
            for (int i = 0; i < numRecords; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "k" + i,
                        "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            int beforeRebalance = engine.search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, numRecords).size();

            // Now feed a REBALANCE entry that bumps default to 8
            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 8,
                    Collections.singletonList(t), Collections.singletonList(oldIx));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(d));
            engine.awaitPendingWorkForTest();

            // Insert another batch — old index keeps N=2, instance 1 still owns its
            // shard; the local count must grow proportionally, NOT reset.
            for (int i = numRecords; i < 2 * numRecords; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "k" + i,
                        "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 1000 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            int afterRebalance = engine.search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, 2 * numRecords).size();

            // Routing did not change for the existing index, so this instance
            // continues to own the same fraction (~half under N=2).
            assertEquals("routing for an existing index must not change after REBALANCE",
                    beforeRebalance * 2, afterRebalance, beforeRebalance / 2);
        }
    }
}
