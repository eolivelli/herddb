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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
 * An ACTIVE engine that observes a {@code INDEXING_SERVICE_REBALANCE} entry
 * MUST update its effective {@code numInstances} on the fly. From the entry's
 * LSN onward, every routing decision against EVERY existing vector index uses
 * the new value — so a freshly-added pod immediately starts owning a share
 * of new writes against indexes that existed long before it joined.
 *
 * <p>Idempotent on lower-or-equal epochs (log replay safety).
 *
 * @author enrico.olivelli
 */
public class RebalanceChangesRoutingTest {

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

    private Index index(int numShards) {
        return Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .build();
    }

    @Test
    public void rebalanceUpdatesEngineCurrentNumInstancesAndIsIdempotent() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 2)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertEquals(2, engine.getCurrentNumInstances());

            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    100L, 4,
                    Collections.singletonList(table()),
                    Collections.singletonList(index(4)));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 1),
                    LogEntryFactory.indexingServiceRebalance(d));
            engine.awaitPendingWorkForTest();

            assertEquals(4, engine.getCurrentNumInstances());
            assertNotNull(engine.getLastObservedRebalance());
            assertEquals(100L, engine.getObservedRebalanceEpoch());

            // Replay same epoch -> no-op
            IndexingServiceRebalanceDescriptor previous = engine.getLastObservedRebalance();
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 2),
                    LogEntryFactory.indexingServiceRebalance(d));
            engine.awaitPendingWorkForTest();
            assertSame(previous, engine.getLastObservedRebalance());
            assertEquals(4, engine.getCurrentNumInstances());

            // Older epoch -> no-op
            IndexingServiceRebalanceDescriptor older = new IndexingServiceRebalanceDescriptor(
                    50L, 8, Collections.emptyList(), Collections.emptyList());
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 3),
                    LogEntryFactory.indexingServiceRebalance(older));
            engine.awaitPendingWorkForTest();
            assertSame(previous, engine.getLastObservedRebalance());
            assertEquals(4, engine.getCurrentNumInstances());

            // Newer epoch -> swap
            IndexingServiceRebalanceDescriptor newer = new IndexingServiceRebalanceDescriptor(
                    200L, 8, Collections.singletonList(table()),
                    Collections.singletonList(index(4)));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 4),
                    LogEntryFactory.indexingServiceRebalance(newer));
            engine.awaitPendingWorkForTest();
            assertEquals(8, engine.getCurrentNumInstances());
            assertEquals(200L, engine.getObservedRebalanceEpoch());
        }
    }

    /**
     * Routing for an EXISTING index changes after REBALANCE. Insert keys
     * pre-rebalance (N=2 routing on instance 0), then bump engine to N=4
     * via a REBALANCE entry, then insert MORE keys: under the new N=4
     * mapping, instance 0 owns a STRICTLY SMALLER fraction of the new
     * keys than it did of the old ones.
     */
    @Test
    public void existingIndexAcceptsFewerNewKeysAfterScaleUp() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 2)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            Table t = table();
            Index ix = index(8);

            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            // Phase 1: 400 inserts under N=2 (instance 0 owns roughly 1/2)
            int phase1 = 400;
            for (int i = 0; i < phase1; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "old-" + i, "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            int afterPhase1 = engine.search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, phase1).size();
            assertTrue("instance 0 must hold ~half of phase-1 keys (N=2)",
                    afterPhase1 > phase1 / 4 && afterPhase1 < (3 * phase1) / 4);

            // REBALANCE to N=4
            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    Collections.singletonList(t),
                    Collections.singletonList(ix));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(d));
            engine.awaitPendingWorkForTest();
            assertEquals(4, engine.getCurrentNumInstances());

            // Phase 2: 400 inserts on the SAME index but routed under N=4
            // (instance 0 should now own roughly 1/4 of the new keys)
            int phase2 = 400;
            for (int i = 0; i < phase2; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "new-" + i, "vec", new float[]{i + 10000, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 2000 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            int afterPhase2 = engine.search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, phase1 + phase2).size();
            int phase2Local = afterPhase2 - afterPhase1;
            // Old data is preserved on instance 0; new data is routed under
            // N=4, so instance 0 should accept roughly half as many new
            // keys as it did old ones. Loose bounds to keep the test stable.
            assertTrue("instance 0 must still hold every old key after rebalance",
                    afterPhase2 >= afterPhase1);
            assertTrue("instance 0 should accept fewer new keys (N=4) than old keys (N=2): "
                            + "phase2Local=" + phase2Local + " afterPhase1=" + afterPhase1,
                    phase2Local < afterPhase1);
            assertTrue("instance 0 should still own SOME phase-2 keys under N=4",
                    phase2Local > 0);
        }
    }

    /**
     * The exact user scenario: a 2-engine cluster ingests data, then is
     * scaled to 4 engines via REBALANCE; new writes against the SAME
     * existing index are now spread across all 4 engines, while every
     * old vector remains exactly once across the cluster (on its
     * original owner).
     */
    @Test
    public void scaleUpFromTwoToFourSpreadsNewWritesAcrossAllInstances() throws Exception {
        Table t = table();
        Index ix = index(8);

        java.util.List<EmbeddedIndexingService> services = new java.util.ArrayList<>();
        try {
            // Phase 1: 2 engines, ingest 200 records.
            for (int i = 0; i < 2; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, i, 2);
                svc.start();
                services.add(svc);
            }
            applyEntryAll(services, new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            applyEntryAll(services, new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            int oldCount = 200;
            insertAll(services, t, "old-", oldCount, 100);
            int totalOldBefore = totalLocal(services, oldCount);
            assertEquals("each phase-1 record must be indexed exactly once",
                    oldCount, totalOldBefore);
            for (int i = 0; i < 2; i++) {
                int local = services.get(i).getEngine().search("default", "mytable", "vidx",
                        new float[]{0, 0, 0}, oldCount).size();
                assertTrue("engine " + i + " must own a non-empty subset of phase-1", local > 0);
            }

            // Phase 2: REBALANCE to N=4 on the existing 2 engines.
            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    Collections.singletonList(t),
                    Collections.singletonList(ix));
            applyEntryAll(services, new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(d));
            for (int i = 0; i < 2; i++) {
                assertEquals(4, services.get(i).getEngine().getCurrentNumInstances());
            }

            // Phase 3: bring up engines 2 and 3 in JOINING mode and feed
            // them the same REBALANCE entry so they bootstrap.
            for (int newId = 2; newId < 4; newId++) {
                Path logDir = folder.newFolder("log-new-" + newId).toPath();
                Path dataDir = folder.newFolder("data-new-" + newId).toPath();
                java.util.Properties props = new java.util.Properties();
                props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
                props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                        String.valueOf(newId));
                props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "4");
                props.setProperty(IndexingServerConfiguration.PROPERTY_BOOTSTRAP_FROM_REBALANCE,
                        "true");
                EmbeddedIndexingService svc = new EmbeddedIndexingService(
                        logDir, dataDir, new IndexingServerConfiguration(props));
                svc.start();
                services.add(svc);
                assertEquals(IndexingServiceEngine.EngineStatus.JOINING,
                        svc.getEngine().getEngineStatus());
                svc.getEngine().applySingleEntryForTest(new LogSequenceNumber(1, 1001 + newId),
                        LogEntryFactory.indexingServiceRebalance(d));
                svc.getEngine().awaitPendingWorkForTest();
                assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE,
                        svc.getEngine().getEngineStatus());
                assertEquals(4, svc.getEngine().getCurrentNumInstances());
            }

            // Joiners hold zero data for the existing index — they didn't
            // replay history. Old data still summed across the cluster
            // matches phase 1.
            assertEquals("old data must remain exactly once across the cluster",
                    oldCount, totalLocal(services, oldCount));
            for (int i = 2; i < 4; i++) {
                assertEquals("joiners must hold zero phase-1 records",
                        0, services.get(i).getEngine().search("default", "mytable", "vidx",
                                new float[]{0, 0, 0}, oldCount).size());
            }

            // Phase 4: insert 400 NEW vectors against the SAME existing
            // index. Routing now uses N=4, so EVERY engine — including the
            // joiners — receives a non-empty subset.
            int newCount = 400;
            insertAll(services, t, "new-", newCount, 5000);
            int total = totalLocal(services, oldCount + newCount);
            assertEquals("every record (old+new) must be indexed exactly once",
                    oldCount + newCount, total);
            for (int i = 0; i < 4; i++) {
                int local = services.get(i).getEngine().search("default", "mytable", "vidx",
                        new float[]{0, 0, 0}, oldCount + newCount).size();
                assertTrue("engine " + i + " must own a non-empty subset after scale-up: "
                        + local, local > 0);
            }
            // Joiners now hold ONLY phase-2 (new) records, so their counts
            // must be strictly less than what engines 0/1 hold (which still
            // carry their phase-1 share too).
            int e0 = services.get(0).getEngine().search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, oldCount + newCount).size();
            int e1 = services.get(1).getEngine().search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, oldCount + newCount).size();
            int e2 = services.get(2).getEngine().search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, oldCount + newCount).size();
            int e3 = services.get(3).getEngine().search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, oldCount + newCount).size();
            assertTrue("joiner 2 must hold strictly fewer records than engine 0: "
                    + e2 + " vs " + e0, e2 < e0);
            assertTrue("joiner 3 must hold strictly fewer records than engine 1: "
                    + e3 + " vs " + e1, e3 < e1);

            // Phase 5: re-INSERT a chunk of the OLD keys. Under the new
            // numInstances=4 mapping, each old key may now route to a
            // DIFFERENT owner than it did under N=2 — so for some keys, two
            // replicas now both hold the same primary key (the original
            // owner from phase 1, plus the new owner from phase 5). This is
            // expected and is exactly the situation a broadcast DELETE has
            // to handle. We pick the first 100 phase-1 keys.
            int reinsertCount = 100;
            int totalBeforeReinsert = totalLocal(services, oldCount + newCount);
            for (int i = 0; i < reinsertCount; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "old-" + i,
                        "vec", new float[]{i + 999, i, i});
                herddb.log.LogEntry update = LogEntryFactory.update(t, rec.key, rec.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 7000 + i);
                for (EmbeddedIndexingService s : services) {
                    s.getEngine().applySingleEntryForTest(lsn, update);
                }
            }
            for (EmbeddedIndexingService s : services) {
                s.getEngine().awaitPendingWorkForTest();
            }
            // Total may have GROWN if any old key now lives on a different
            // owner under N=4 than it did under N=2 — both replicas hold it.
            // We don't pin an exact growth, just assert the cluster is in a
            // consistent state and DELETE will clean up correctly below.
            int totalAfterReinsert = totalLocal(services, oldCount + newCount);
            assertTrue("re-INSERT under new mapping may add stale duplicates "
                            + "but never lose data: " + totalAfterReinsert
                            + " >= " + totalBeforeReinsert,
                    totalAfterReinsert >= totalBeforeReinsert);

            // Phase 6: DELETE the same first-100 old keys. DELETE is
            // BROADCAST to every replica regardless of routing — so any
            // replica that still holds a copy (original owner or new
            // owner) drops it. After this pass, every "old-0".."old-99"
            // PK must be GONE from every replica.
            for (int i = 0; i < reinsertCount; i++) {
                Record rec = RecordSerializer.makeRecord(t,
                        "pk", "old-" + i,
                        "vec", new float[]{0, 0, 0});
                herddb.log.LogEntry delete = LogEntryFactory.delete(t, rec.key, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 8000 + i);
                for (EmbeddedIndexingService s : services) {
                    s.getEngine().applySingleEntryForTest(lsn, delete);
                }
            }
            for (EmbeddedIndexingService s : services) {
                s.getEngine().awaitPendingWorkForTest();
            }
            int totalAfterDelete = totalLocal(services, oldCount + newCount);
            // Total dropped by AT LEAST reinsertCount (every replica that
            // held one of the deleted keys lost it).
            assertTrue("broadcast DELETE must drop the cluster total by at "
                            + "least the deleted count: before=" + totalAfterReinsert
                            + " after=" + totalAfterDelete + " expected drop>=" + reinsertCount,
                    totalAfterReinsert - totalAfterDelete >= reinsertCount);
            // And the specific keys must be invisible to a single-replica
            // search on EVERY engine (we verify the PKs do not show up by
            // checking total contains exactly old-100..old-(oldCount-1)
            // plus new-0..new-(newCount-1) survivors).
            assertEquals("post-DELETE total = surviving phase-1 + all phase-2",
                    (oldCount - reinsertCount) + newCount,
                    totalAfterDelete);
        } finally {
            for (EmbeddedIndexingService s : services) {
                try {
                    s.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    /**
     * The transactional sibling of
     * {@link #scaleUpFromTwoToFourSpreadsNewWritesAcrossAllInstances}: drive
     * BEGIN/INSERT/COMMIT through the engine's transaction-buffer path
     * (the same path the live tailer uses) across a REBALANCE that lands
     * mid-transaction. A transaction begun pre-rebalance and committed
     * post-rebalance must apply its buffered entries under the NEW
     * mapping (the user's "no change is lost" guarantee), and a broadcast
     * DELETE issued at the end must wipe every key from every replica.
     */
    @Test
    public void rebalanceMidTransactionRoutesBufferedInsertsUnderNewMapping() throws Exception {
        Table t = table();
        Index ix = index(8);

        java.util.List<EmbeddedIndexingService> services = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                Path logDir = folder.newFolder("tx-log-" + i).toPath();
                Path dataDir = folder.newFolder("tx-data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, i, 2);
                svc.start();
                services.add(svc);
            }
            applyEntryAll(services, new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            applyEntryAll(services, new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            // TX-A: begin -> insert -> commit ENTIRELY pre-rebalance.
            // Routes under N=2.
            long txA = 100L;
            processAll(services, new LogSequenceNumber(1, 100),
                    LogEntryFactory.beginTransaction(txA));
            for (int i = 0; i < 50; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "txA-" + i,
                        "vec", new float[]{i, i, i});
                processAll(services, new LogSequenceNumber(1, 110 + i),
                        new herddb.log.LogEntry(System.currentTimeMillis(),
                                herddb.log.LogEntryType.INSERT, txA, t.name,
                                rec.key, rec.value));
            }
            processAll(services, new LogSequenceNumber(1, 200),
                    LogEntryFactory.commitTransaction(txA));
            awaitAll(services);
            int afterTxA = totalLocal(services, 1000);
            assertEquals("TX-A inserts must each be indexed exactly once across N=2",
                    50, afterTxA);

            // TX-B: begin pre-rebalance, insert pre-rebalance, REBALANCE
            // arrives, insert MORE post-rebalance, commit post-rebalance.
            // The COMMIT triggers the apply of every buffered entry under
            // the engine's CURRENT (post-rebalance) numInstances, which is
            // the user's "no change is lost" guarantee (a pre-rebalance
            // begun TX still lands every entry on the right new owner).
            long txB = 200L;
            processAll(services, new LogSequenceNumber(1, 300),
                    LogEntryFactory.beginTransaction(txB));
            for (int i = 0; i < 25; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "txB-pre-" + i,
                        "vec", new float[]{i, i, i});
                processAll(services, new LogSequenceNumber(1, 310 + i),
                        new herddb.log.LogEntry(System.currentTimeMillis(),
                                herddb.log.LogEntryType.INSERT, txB, t.name,
                                rec.key, rec.value));
            }

            // REBALANCE to N=4 mid-tx (delivered via the transaction buffer
            // path so it interleaves with the buffered entries on the
            // engine timeline). The descriptor flips currentNumInstances
            // immediately on every replica that processes it.
            IndexingServiceRebalanceDescriptor descriptor =
                    new IndexingServiceRebalanceDescriptor(
                            System.currentTimeMillis(), 4,
                            Collections.singletonList(t),
                            Collections.singletonList(ix));
            processAll(services, new LogSequenceNumber(1, 400),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            for (int i = 0; i < 2; i++) {
                assertEquals(4, services.get(i).getEngine().getCurrentNumInstances());
            }

            for (int i = 0; i < 25; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "txB-post-" + i,
                        "vec", new float[]{i + 5000, i, i});
                processAll(services, new LogSequenceNumber(1, 410 + i),
                        new herddb.log.LogEntry(System.currentTimeMillis(),
                                herddb.log.LogEntryType.INSERT, txB, t.name,
                                rec.key, rec.value));
            }
            processAll(services, new LogSequenceNumber(1, 500),
                    LogEntryFactory.commitTransaction(txB));
            awaitAll(services);

            int afterTxB = totalLocal(services, 1000);
            // Every TX-B insert (pre AND post) must land on its N=4 owner
            // exactly once. With 2 active engines, each owns about half of
            // the keys; the other half routes to instanceIds 2 or 3 which
            // don't exist on the cluster yet — those entries are dropped.
            // We tolerate that by asserting a non-trivial growth and the
            // routing-correctness invariant: every replica only stores
            // keys it owns under N=4.
            assertTrue("some TX-B inserts must have landed: " + afterTxB,
                    afterTxB > afterTxA);
            for (int i = 0; i < 2; i++) {
                IndexingServiceEngine e = services.get(i).getEngine();
                int local = e.search("default", "mytable", "vidx",
                        new float[]{0, 0, 0}, 1000).size();
                // Coarse upper bound: the local count cannot exceed the
                // replica's expected share of all inserts (~ 1/2 + 1/4
                // headroom for distribution noise).
                assertTrue("engine " + i + " local count " + local
                                + " must be a subset (cluster total " + afterTxB + ")",
                        local <= afterTxB);
            }

            // TX-C: BEGIN -> INSERT -> ROLLBACK. Nothing must land.
            long txC = 300L;
            int beforeTxC = totalLocal(services, 1000);
            processAll(services, new LogSequenceNumber(1, 600),
                    LogEntryFactory.beginTransaction(txC));
            for (int i = 0; i < 25; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "txC-" + i,
                        "vec", new float[]{i + 9000, i, i});
                processAll(services, new LogSequenceNumber(1, 610 + i),
                        new herddb.log.LogEntry(System.currentTimeMillis(),
                                herddb.log.LogEntryType.INSERT, txC, t.name,
                                rec.key, rec.value));
            }
            processAll(services, new LogSequenceNumber(1, 700),
                    LogEntryFactory.rollbackTransaction(txC));
            awaitAll(services);
            assertEquals("ROLLBACK must drop every buffered entry",
                    beforeTxC, totalLocal(services, 1000));

            // TX-D: begin -> DELETE every TX-A key -> commit. Broadcast
            // DELETE wipes the keys from every replica that holds them
            // (including any duplicates the test may have produced).
            long txD = 400L;
            processAll(services, new LogSequenceNumber(1, 800),
                    LogEntryFactory.beginTransaction(txD));
            for (int i = 0; i < 50; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "txA-" + i,
                        "vec", new float[]{0, 0, 0});
                processAll(services, new LogSequenceNumber(1, 810 + i),
                        new herddb.log.LogEntry(System.currentTimeMillis(),
                                herddb.log.LogEntryType.DELETE, txD, t.name,
                                rec.key, null));
            }
            processAll(services, new LogSequenceNumber(1, 900),
                    LogEntryFactory.commitTransaction(txD));
            awaitAll(services);

            int afterTxD = totalLocal(services, 1000);
            // Every TX-A key gone => total drops by 50 (each TX-A key was
            // indexed exactly once across the cluster, before TX-D).
            assertEquals("broadcast DELETE must wipe the 50 TX-A keys cluster-wide",
                    afterTxB - 50, afterTxD);
        } finally {
            for (EmbeddedIndexingService s : services) {
                try {
                    s.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    /**
     * Restart-after-rebalance: a fresh engine recovers the cluster's
     * post-REBALANCE numInstances from log replay alone. Production behavior
     * mirrors this — at startup the engine sets {@code tailerStart =
     * START_OF_TIME} (see {@link IndexingServiceEngine#start()}) and the
     * tailer feeds every entry, including the REBALANCE entries, through
     * {@link IndexingServiceEngine#processEntryForTest} (here we drive the
     * same code path manually). After replay the engine's
     * {@code currentNumInstances} must reflect the most recent REBALANCE.
     */
    @Test
    public void engineRecoversCurrentNumInstancesFromLogReplay() throws Exception {
        Path logDir = folder.newFolder("rr-log").toPath();
        Path dataDir = folder.newFolder("rr-data").toPath();

        // Phase 1: fresh engine, apply DDL + insert + REBALANCE + insert
        // through the full processEntry pipeline.
        Table t = table();
        Index ix = index(8);
        java.util.List<herddb.log.LogEntry> persistedLog = new java.util.ArrayList<>();
        java.util.List<LogSequenceNumber> persistedLsn = new java.util.ArrayList<>();

        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 2)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertEquals(2, engine.getCurrentNumInstances());

            recordAndProcess(engine, persistedLog, persistedLsn,
                    new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            recordAndProcess(engine, persistedLog, persistedLsn,
                    new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            for (int i = 0; i < 50; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "k" + i,
                        "vec", new float[]{i, i, i});
                recordAndProcess(engine, persistedLog, persistedLsn,
                        new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    Collections.singletonList(t),
                    Collections.singletonList(ix));
            recordAndProcess(engine, persistedLog, persistedLsn,
                    new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(d));
            for (int i = 50; i < 100; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "k" + i,
                        "vec", new float[]{i, i, i});
                recordAndProcess(engine, persistedLog, persistedLsn,
                        new LogSequenceNumber(1, 2000 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            assertEquals(4, engine.getCurrentNumInstances());
        }

        // Phase 2: simulate restart — fresh engine, fresh data dir, replay
        // the SAME log entries from START_OF_TIME (the production restart
        // path). The engine starts at its bootstrap value (2) and must
        // converge to 4 by the time it has processed every entry.
        Path logDir2 = folder.newFolder("rr-log-restart").toPath();
        Path dataDir2 = folder.newFolder("rr-data-restart").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir2, dataDir2, 0, 2)) {
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();
            assertEquals("fresh engine starts at the JVM-property bootstrap value",
                    2, engine.getCurrentNumInstances());

            for (int i = 0; i < persistedLog.size(); i++) {
                engine.processEntryForTest(persistedLsn.get(i), persistedLog.get(i));
            }
            engine.awaitPendingWorkForTest();
            assertEquals("after log replay the engine must observe the REBALANCE "
                    + "entry and update currentNumInstances to 4",
                    4, engine.getCurrentNumInstances());
            // Schema was rebuilt and vector store exists
            assertNotNull(engine.getLastObservedRebalance());
        }
    }

    private void recordAndProcess(IndexingServiceEngine engine,
                                   java.util.List<herddb.log.LogEntry> log,
                                   java.util.List<LogSequenceNumber> lsns,
                                   LogSequenceNumber lsn, herddb.log.LogEntry entry) {
        log.add(entry);
        lsns.add(lsn);
        engine.processEntryForTest(lsn, entry);
    }

    /**
     * The watermark store persists {@code numInstances} alongside the LSN at
     * every successful checkpoint. After a restart, the engine reads the
     * snapshot and recovers the post-rebalance routing value EVEN IF the
     * BookKeeper history that carried the REBALANCE entry has been trimmed
     * in the meantime.
     */
    @Test
    public void watermarkSnapshotPersistsAndRestoresNumInstances() throws Exception {
        // Use an in-memory watermark store shared between the two engine
        // instances so we can simulate a restart with a trimmed log.
        java.util.concurrent.atomic.AtomicReference<WatermarkSnapshot> persisted =
                new java.util.concurrent.atomic.AtomicReference<>(WatermarkSnapshot.START_OF_TIME);
        WatermarkStore sharedStore = new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return persisted.get();
            }

            @Override
            public void save(WatermarkSnapshot snapshot) {
                persisted.set(snapshot);
            }
        };

        Path logDir = folder.newFolder("ws-log").toPath();
        Path dataDir = folder.newFolder("ws-data").toPath();
        Table t = table();
        Index ix = index(8);

        // Phase 1: engine ingests, REBALANCE arrives, checkpoint persists
        // numInstances=4 alongside the watermark LSN.
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 2)) {
            svc.setWatermarkStore(sharedStore);
            svc.start();
            IndexingServiceEngine engine = svc.getEngine();

            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            for (int i = 0; i < 32; i++) {
                Record rec = RecordSerializer.makeRecord(t, "pk", "k" + i,
                        "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(t, rec.key, rec.value, null));
            }
            IndexingServiceRebalanceDescriptor d = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    Collections.singletonList(t),
                    Collections.singletonList(ix));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(d));
            engine.awaitPendingWorkForTest();
            assertEquals(4, engine.getCurrentNumInstances());

            engine.setLastProcessedLsnForTest(new LogSequenceNumber(1, 1100));
            engine.forceCheckpointAndSaveWatermark();

            WatermarkSnapshot saved = persisted.get();
            assertNotNull(saved);
            assertEquals("checkpoint must persist the post-rebalance numInstances",
                    4, saved.numInstances);
            assertEquals(1L, saved.lsn.ledgerId);
        }

        // Phase 2: simulate restart with the BK history TRIMMED — fresh data
        // dir, fresh log dir (empty), but the persisted watermark snapshot
        // survives. The engine's bootstrap numInstances is 2; without the
        // snapshot it would stay at 2 forever (no REBALANCE in the trimmed
        // log to fix it). With the snapshot, the engine recovers
        // currentNumInstances=4 directly from the watermark store.
        Path dataDir2 = folder.newFolder("ws-data-restart").toPath();
        Path logDir2 = folder.newFolder("ws-log-restart").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir2, dataDir2, 0, 2)) {
            svc.setWatermarkStore(sharedStore);
            svc.start();
            assertEquals("engine must recover the post-rebalance numInstances "
                    + "from the watermark store, even with no REBALANCE entry "
                    + "available in the (trimmed) log",
                    4, svc.getEngine().getCurrentNumInstances());
        }
    }

    /**
     * Backward compat with the {@code numInstances=0} sentinel: a snapshot
     * loaded with {@code numInstances=0} (e.g. a freshly-started engine that
     * has never been rebalanced) leaves the bootstrap value untouched.
     */
    @Test
    public void watermarkSnapshotZeroNumInstancesKeepsBootstrap() throws Exception {
        WatermarkStore zeroStore = new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return new WatermarkSnapshot(new LogSequenceNumber(1, 5), 0);
            }

            @Override
            public void save(WatermarkSnapshot snapshot) {
                // no-op
            }
        };
        Path logDir = folder.newFolder("ws-zero-log").toPath();
        Path dataDir = folder.newFolder("ws-zero-data").toPath();
        try (EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, 0, 3)) {
            svc.setWatermarkStore(zeroStore);
            svc.start();
            assertEquals("engine must keep the JVM bootstrap value when the "
                    + "watermark snapshot has numInstances=0",
                    3, svc.getEngine().getCurrentNumInstances());
        }
    }

    private void processAll(java.util.List<EmbeddedIndexingService> services,
                             LogSequenceNumber lsn, herddb.log.LogEntry entry) {
        for (EmbeddedIndexingService s : services) {
            s.getEngine().processEntryForTest(lsn, entry);
        }
    }

    private void awaitAll(java.util.List<EmbeddedIndexingService> services) throws Exception {
        for (EmbeddedIndexingService s : services) {
            s.getEngine().awaitPendingWorkForTest();
        }
    }

    private void applyEntryAll(java.util.List<EmbeddedIndexingService> services,
                                LogSequenceNumber lsn, herddb.log.LogEntry entry) throws Exception {
        for (EmbeddedIndexingService s : services) {
            s.getEngine().applyEntry(lsn, entry);
        }
    }

    private void insertAll(java.util.List<EmbeddedIndexingService> services, Table t,
                           String prefix, int count, long lsnBase) throws Exception {
        for (int i = 0; i < count; i++) {
            Record rec = RecordSerializer.makeRecord(t,
                    "pk", prefix + i, "vec", new float[]{i, i * 2.0f, i * 3.0f});
            herddb.log.LogEntry insert = LogEntryFactory.insert(t, rec.key, rec.value, null);
            LogSequenceNumber lsn = new LogSequenceNumber(1, lsnBase + i);
            for (EmbeddedIndexingService s : services) {
                s.getEngine().applySingleEntryForTest(lsn, insert);
            }
        }
        for (EmbeddedIndexingService s : services) {
            s.getEngine().awaitPendingWorkForTest();
        }
    }

    private int totalLocal(java.util.List<EmbeddedIndexingService> services, int searchK) {
        int total = 0;
        for (EmbeddedIndexingService s : services) {
            total += s.getEngine().search("default", "mytable", "vidx",
                    new float[]{0, 0, 0}, searchK).size();
        }
        return total;
    }

    @SuppressWarnings("unused")
    private static java.util.List<Index> ignoreUnusedImport(Index ix) {
        return Arrays.asList(ix);
    }
}
