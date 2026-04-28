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
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.log.IndexingServiceRebalanceDescriptor;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage of the indexing-service scale-up flow at the engine
 * level (file commit log; no BookKeeper or ZooKeeper).
 *
 * <p>The scenario mirrors a production scale-up:
 * <ol>
 *   <li>Cluster boots with 2 primary engines (instanceId 0, 1). A vector
 *       index is created with {@code numInstances=2} and ~200 records are
 *       inserted; both engines hold disjoint subsets that sum to the full
 *       set.</li>
 *   <li>Operator runs {@code EXECUTE INDEXING_SERVICE_REBALANCE 4}: the
 *       active engines record the new descriptor (no behavior change for
 *       existing indexes since each index's {@code numInstances} is
 *       permanent).</li>
 *   <li>Two more engines (instanceId 2, 3) are added with
 *       {@code bootstrapFromRebalance=true}. They boot in JOINING, then
 *       observe the REBALANCE entry that carries the schema and transition
 *       to ACTIVE.</li>
 *   <li>A SECOND vector index is created with {@code numInstances=4}; new
 *       inserts route across all 4 engines, while the original index's
 *       data remains untouched on engines 0 and 1.</li>
 * </ol>
 *
 * @author enrico.olivelli
 */
public class RebalanceScaleUpEndToEndTest {

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

    private Index vectorIndex(String name, int numShards, int numInstances) {
        return Index.builder()
                .name(name)
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .property(VectorIndexManager.PROP_NUM_INSTANCES, String.valueOf(numInstances))
                .build();
    }

    private float[] vectorFor(int i) {
        return new float[]{i, i * 2.0f, i * 3.0f};
    }

    private EmbeddedIndexingService startEngine(int instanceId, int numInstancesBootstrap,
                                                 boolean joining) throws Exception {
        Path logDir = folder.newFolder("log-" + instanceId + "-" + joining).toPath();
        Path dataDir = folder.newFolder("data-" + instanceId + "-" + joining).toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstancesBootstrap));
        if (joining) {
            props.setProperty(IndexingServerConfiguration.PROPERTY_BOOTSTRAP_FROM_REBALANCE, "true");
        }
        EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir,
                new IndexingServerConfiguration(props));
        svc.start();
        return svc;
    }

    @Test
    public void scaleUpFromTwoToFourPreservesOldIndexAndRoutesNewIndexAcrossAllFour() throws Exception {
        Table t = table();
        Index oldIdx = vectorIndex("vidx_old", 4, 2);

        List<EmbeddedIndexingService> services = new ArrayList<>();
        try {
            // Phase 1: 2 active engines. Apply DDL and insert records.
            for (int i = 0; i < 2; i++) {
                services.add(startEngine(i, 2, false));
            }
            applyEntryAll(services, new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            applyEntryAll(services, new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(oldIdx, null));

            int oldRecords = 200;
            insertAll(services, t, "old-", 0, oldRecords, 100);

            int totalOldBefore = totalLocal(services, "vidx_old", oldRecords);
            assertEquals("every old-index record must be indexed exactly once across 2 engines",
                    oldRecords, totalOldBefore);
            for (int i = 0; i < 2; i++) {
                int local = services.get(i).getEngine().search("default", "mytable", "vidx_old",
                        vectorFor(0), oldRecords).size();
                assertTrue("engine " + i + " must hold a non-empty subset", local > 0);
                assertTrue("engine " + i + " must hold a strict subset", local < oldRecords);
            }

            // Phase 2: REBALANCE to default=4. Active engines record the
            // descriptor; routing of the EXISTING index does not change.
            IndexingServiceRebalanceDescriptor descriptor = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    java.util.Collections.singletonList(t),
                    java.util.Collections.singletonList(oldIdx));
            applyEntryAll(services, new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            for (int i = 0; i < 2; i++) {
                assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE,
                        services.get(i).getEngine().getEngineStatus());
                assertEquals(4, services.get(i).getEngine().getLastObservedRebalance().defaultNumInstances);
            }

            // Phase 3: Two more engines join (instanceId 2, 3). They boot in
            // JOINING and need to see the REBALANCE entry to acquire schema.
            for (int i = 2; i < 4; i++) {
                services.add(startEngine(i, 4, true));
            }
            for (int i = 2; i < 4; i++) {
                assertEquals(IndexingServiceEngine.EngineStatus.JOINING,
                        services.get(i).getEngine().getEngineStatus());
            }
            // Re-deliver the REBALANCE entry to the joiners; they bootstrap
            // schema from it and transition to ACTIVE.
            for (int i = 2; i < 4; i++) {
                services.get(i).getEngine().applySingleEntryForTest(
                        new LogSequenceNumber(1, 1001),
                        LogEntryFactory.indexingServiceRebalance(descriptor));
                services.get(i).getEngine().awaitPendingWorkForTest();
                assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE,
                        services.get(i).getEngine().getEngineStatus());
            }

            // Old-index data on joiners is empty (never replayed). Old-index
            // data on engines 0,1 is intact.
            int totalOldAfter = totalLocal(services, "vidx_old", oldRecords);
            assertEquals("REBALANCE must not move old-index data", oldRecords, totalOldAfter);
            for (int i = 2; i < 4; i++) {
                assertEquals("joiners must hold zero old-index records (history not replayed)",
                        0, services.get(i).getEngine().search("default", "mytable", "vidx_old",
                                vectorFor(0), oldRecords).size());
            }

            // Phase 4: Create a NEW index with numInstances=4 and insert.
            // All four engines must own disjoint subsets summing to the full set.
            Index newIdx = vectorIndex("vidx_new", 4, 4);
            applyEntryAll(services, new LogSequenceNumber(1, 2000),
                    LogEntryFactory.createIndex(newIdx, null));
            int newRecords = 400;
            insertAll(services, t, "new-", 0, newRecords, 3000);

            int totalNew = totalLocal(services, "vidx_new", newRecords);
            assertEquals("every new-index record must be indexed exactly once across 4 engines",
                    newRecords, totalNew);
            for (int i = 0; i < 4; i++) {
                int local = services.get(i).getEngine().search("default", "mytable", "vidx_new",
                        vectorFor(0), newRecords).size();
                assertTrue("engine " + i + " must hold a non-empty subset for new index",
                        local > 0);
                assertTrue("engine " + i + " must hold a strict subset for new index",
                        local < newRecords);
            }

            // The OLD index continues to receive every insert routed under
            // its permanent N=2 mapping — so engines 0 and 1 now hold the
            // union of phase-1 (200) + phase-4 (400) inserts under vidx_old.
            // Joiners 2 and 3 still hold zero records for vidx_old because
            // their instanceId is outside the N=2 owner set.
            int totalOldFinal = totalLocal(services, "vidx_old",
                    oldRecords + newRecords);
            assertEquals("old index keeps all routed inserts under N=2",
                    oldRecords + newRecords, totalOldFinal);
            for (int i = 2; i < 4; i++) {
                assertEquals("joiners still hold zero old-index records",
                        0, services.get(i).getEngine().search("default", "mytable", "vidx_old",
                                vectorFor(0), oldRecords + newRecords).size());
            }
        } finally {
            for (EmbeddedIndexingService s : services) {
                try {
                    s.close();
                } catch (Exception ignored) {
                    // cleanup best-effort
                }
            }
        }
    }

    private void applyEntryAll(List<EmbeddedIndexingService> services,
                                LogSequenceNumber lsn, LogEntry entry) throws Exception {
        for (EmbeddedIndexingService s : services) {
            s.getEngine().applyEntry(lsn, entry);
        }
    }

    private void insertAll(List<EmbeddedIndexingService> services, Table t,
                           String keyPrefix, int from, int count, long lsnBase) throws Exception {
        for (int i = from; i < from + count; i++) {
            Record rec = RecordSerializer.makeRecord(t,
                    "pk", keyPrefix + i,
                    "vec", vectorFor(i));
            LogEntry insert = LogEntryFactory.insert(t, rec.key, rec.value, null);
            LogSequenceNumber lsn = new LogSequenceNumber(1, lsnBase + i);
            for (EmbeddedIndexingService s : services) {
                s.getEngine().applySingleEntryForTest(lsn, insert);
            }
        }
        for (EmbeddedIndexingService s : services) {
            s.getEngine().awaitPendingWorkForTest();
        }
    }

    private int totalLocal(List<EmbeddedIndexingService> services, String indexName, int searchK)
            throws Exception {
        int total = 0;
        for (EmbeddedIndexingService s : services) {
            total += s.getEngine().search("default", "mytable", indexName, vectorFor(0), searchK).size();
        }
        return total;
    }
}
