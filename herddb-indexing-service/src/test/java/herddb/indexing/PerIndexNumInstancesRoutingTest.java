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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies per-index routing: each vector index uses its own
 * {@link VectorIndexManager#PROP_NUM_INSTANCES} (baked at CREATE INDEX time)
 * to decide ownership, independently of the engine's bootstrap
 * {@code numInstances}. Two indexes on the same table can therefore be
 * sharded across different replica counts and they coexist.
 *
 * @author enrico.olivelli
 */
public class PerIndexNumInstancesRoutingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int VECTOR_DIM = 3;
    private static final int NUM_RECORDS = 200;

    private Table createTable() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createVectorIndex(String name, int numShards, Integer numInstances) {
        Index.Builder b = Index.builder()
                .name(name)
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards));
        if (numInstances != null) {
            b.property(VectorIndexManager.PROP_NUM_INSTANCES, String.valueOf(numInstances));
        }
        return b.build();
    }

    private float[] vectorFor(int i) {
        return new float[]{i * 1.0f, i * 2.0f, i * 3.0f};
    }

    /**
     * The big one: two indexes on the same table, one with N=2 (from
     * PROP_NUM_INSTANCES) and one with N=4. Run on a 4-engine cluster.
     * Index A (N=2) is owned only by instances 0,1; instances 2,3 must hold
     * NOTHING for it. Index B (N=4) is sharded across all four; every
     * instance owns a non-empty subset.
     */
    @Test
    public void perIndexNumInstancesIsRespected() throws Exception {
        int numEngineInstances = 4;
        int numShards = 4;

        List<EmbeddedIndexingService> services = new ArrayList<>();
        List<IndexingServiceEngine> engines = new ArrayList<>();
        try {
            for (int i = 0; i < numEngineInstances; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(
                        logDir, dataDir, i, numEngineInstances);
                svc.start();
                services.add(svc);
                engines.add(svc.getEngine());
            }

            Table table = createTable();
            Index oldIdx = createVectorIndex("vidx_old", numShards, 2);
            Index newIdx = createVectorIndex("vidx_new", numShards, 4);

            applyDdl(engines, table, oldIdx, newIdx);
            applyInserts(engines, table, NUM_RECORDS);

            int totalOld = 0;
            int totalNew = 0;
            for (int i = 0; i < numEngineInstances; i++) {
                int oldLocal = engines.get(i).search("default", "mytable", "vidx_old",
                        vectorFor(0), NUM_RECORDS).size();
                int newLocal = engines.get(i).search("default", "mytable", "vidx_new",
                        vectorFor(0), NUM_RECORDS).size();
                totalOld += oldLocal;
                totalNew += newLocal;

                if (i >= 2) {
                    // Old index (N=2) must NOT have routed any inserts to instances 2,3
                    assertEquals("instance " + i + " must hold zero records for vidx_old (N=2)",
                            0, oldLocal);
                }
                // New index (N=4) must have routed at least one insert to every instance
                assertTrue("instance " + i + " must hold a non-empty subset for vidx_new (N=4) but had "
                        + newLocal, newLocal > 0);
                assertTrue("instance " + i + " must hold a STRICT subset for vidx_new (N=4) but had "
                        + newLocal + " of " + NUM_RECORDS, newLocal < NUM_RECORDS);
            }
            assertEquals("vidx_old: every record must be indexed exactly once across the cluster",
                    NUM_RECORDS, totalOld);
            assertEquals("vidx_new: every record must be indexed exactly once across the cluster",
                    NUM_RECORDS, totalNew);
        } finally {
            closeAll(services);
        }
    }

    /**
     * Backward compatibility: an index without PROP_NUM_INSTANCES falls back to
     * the engine's JVM-property bootstrap {@code numInstances}. Verified by
     * comparing the legacy single-property index against an explicitly-stamped
     * one with the same N — the routing must be identical.
     */
    @Test
    public void missingPropFallsBackToEngineBootstrap() throws Exception {
        int numEngineInstances = 3;
        int numShards = 6;

        List<EmbeddedIndexingService> services = new ArrayList<>();
        List<IndexingServiceEngine> engines = new ArrayList<>();
        try {
            for (int i = 0; i < numEngineInstances; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(
                        logDir, dataDir, i, numEngineInstances);
                svc.start();
                services.add(svc);
                engines.add(svc.getEngine());
            }

            Table table = createTable();
            Index legacyIdx = createVectorIndex("vidx_legacy", numShards, null);
            Index stampedIdx = createVectorIndex("vidx_stamped", numShards, numEngineInstances);

            applyDdl(engines, table, legacyIdx, stampedIdx);
            applyInserts(engines, table, NUM_RECORDS);

            int totalLegacy = 0;
            int totalStamped = 0;
            for (int i = 0; i < numEngineInstances; i++) {
                int legacyLocal = engines.get(i).search("default", "mytable", "vidx_legacy",
                        vectorFor(0), NUM_RECORDS).size();
                int stampedLocal = engines.get(i).search("default", "mytable", "vidx_stamped",
                        vectorFor(0), NUM_RECORDS).size();
                assertEquals("legacy and stamped indexes must route identically when N matches "
                        + "the bootstrap value (instance " + i + ")",
                        stampedLocal, legacyLocal);
                totalLegacy += legacyLocal;
                totalStamped += stampedLocal;
            }
            assertEquals(NUM_RECORDS, totalLegacy);
            assertEquals(NUM_RECORDS, totalStamped);
        } finally {
            closeAll(services);
        }
    }

    /**
     * UPDATE on a key for which this instance is not the per-index owner must
     * not create a phantom vector. Pre-fix, applyUpdate unconditionally called
     * addVector after a no-op removeVector, leaking phantoms onto every
     * non-owner replica.
     */
    @Test
    public void updateDoesNotCreatePhantomOnNonOwner() throws Exception {
        int numEngineInstances = 4;
        int numShards = 4;

        List<EmbeddedIndexingService> services = new ArrayList<>();
        List<IndexingServiceEngine> engines = new ArrayList<>();
        try {
            for (int i = 0; i < numEngineInstances; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(
                        logDir, dataDir, i, numEngineInstances);
                svc.start();
                services.add(svc);
                engines.add(svc.getEngine());
            }

            Table table = createTable();
            Index oldIdx = createVectorIndex("vidx_old", numShards, 2);

            applyDdl(engines, table, oldIdx);
            applyInserts(engines, table, NUM_RECORDS);

            // Rewrite every record via an UPDATE entry with a different vector
            for (int i = 0; i < NUM_RECORDS; i++) {
                float[] vec = new float[]{i * 1.0f + 100, i * 2.0f + 100, i * 3.0f + 100};
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", vec);
                LogEntry update = LogEntryFactory.update(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(2, 10 + i);
                for (IndexingServiceEngine engine : engines) {
                    engine.applySingleEntryForTest(lsn, update);
                }
            }
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // Instances 2 and 3 must STILL hold zero records — UPDATEs went to
            // owners only, no phantoms leaked here.
            for (int i = 2; i < numEngineInstances; i++) {
                int local = engines.get(i).search("default", "mytable", "vidx_old",
                        vectorFor(0), NUM_RECORDS).size();
                assertFalse("instance " + i + " accumulated phantom vectors via UPDATE: " + local,
                        local > 0);
            }
            // The two owner instances together still hold every record exactly once
            int totalOwnerSide = engines.get(0).search("default", "mytable", "vidx_old",
                    vectorFor(0), NUM_RECORDS).size()
                    + engines.get(1).search("default", "mytable", "vidx_old",
                            vectorFor(0), NUM_RECORDS).size();
            assertEquals(NUM_RECORDS, totalOwnerSide);
        } finally {
            closeAll(services);
        }
    }

    private void applyDdl(List<IndexingServiceEngine> engines, Table table, Index... indexes) throws Exception {
        LogEntry createTable = LogEntryFactory.createTable(table, null);
        long offset = 1;
        for (IndexingServiceEngine engine : engines) {
            engine.applyEntry(new LogSequenceNumber(1, offset), createTable);
        }
        offset++;
        for (Index idx : indexes) {
            LogEntry createIndex = LogEntryFactory.createIndex(idx, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, offset), createIndex);
            }
            offset++;
        }
    }

    private void applyInserts(List<IndexingServiceEngine> engines, Table table, int numRecords)
            throws Exception {
        for (int i = 0; i < numRecords; i++) {
            float[] vec = vectorFor(i);
            Record record = RecordSerializer.makeRecord(table,
                    "pk", "key" + i,
                    "vec", vec);
            LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
            LogSequenceNumber lsn = new LogSequenceNumber(1, 100 + i);
            for (IndexingServiceEngine engine : engines) {
                engine.applySingleEntryForTest(lsn, insert);
            }
        }
        for (IndexingServiceEngine engine : engines) {
            engine.awaitPendingWorkForTest();
        }
    }

    private void closeAll(List<EmbeddedIndexingService> services) {
        for (EmbeddedIndexingService svc : services) {
            try {
                svc.close();
            } catch (Exception e) {
                // ignore cleanup errors
            }
        }
    }
}
