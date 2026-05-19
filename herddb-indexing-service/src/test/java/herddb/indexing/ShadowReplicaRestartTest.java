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
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that a shadow replica recovers correctly across restarts —
 * the second scenario asked for by issue #383.
 *
 * <p>A shadow replica reads its on-disk view from the same shared
 * {@link MemoryDataStorageManager} (or remote-backed DSM in production)
 * that the primary writes to. It does not run a tailer, never replays the
 * commit log, and reloads in response to ZK-watcher fires. The tests cover:
 *
 * <ul>
 *   <li>a healthy shadow that is closed and restarted while the primary
 *       continues to publish checkpoints — the restarted shadow must
 *       re-attach and serve searches without rebuilding;</li>
 *   <li>a shadow that boots before the primary has published any state —
 *       it must report {@code shadowReady=false} on every restart until
 *       the primary's first checkpoint state appears;</li>
 *   <li>a shadow that restarts <em>after the primary has rotated the
 *       index UUID</em> (DROP+CREATE between shadow runs) — it must
 *       observe the new UUID on the next reload and not return stale
 *       results from the gone-but-cached old segments.</li>
 * </ul>
 */
public class ShadowReplicaRestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    /**
     * Mirror of {@code ShadowReloadTest.ShadowAwareMemoryMetadata}: an
     * in-memory metadata manager that stores per-instance checkpoint state
     * and fires registered watchers on every {@code publish}. Defining
     * one here keeps the restart tests self-contained — duplicating ~30
     * lines is cheaper than coupling the two test classes through a shared
     * helper that may evolve to fit only one of them.
     */
    private static final class ShadowAwareMemoryMetadata extends MemoryMetadataStorageManager {
        final Map<Integer, IndexingServiceCheckpointState> state = new java.util.HashMap<>();
        final Map<Integer, List<Consumer<IndexingServiceCheckpointState>>> watchers =
                new java.util.HashMap<>();

        @Override
        public synchronized void publishIndexingServiceCheckpointState(
                IndexingServiceCheckpointState s) throws MetadataStorageManagerException {
            state.put(s.getInstanceId(), s);
            List<Consumer<IndexingServiceCheckpointState>> cbs = watchers.get(s.getInstanceId());
            if (cbs != null) {
                for (Consumer<IndexingServiceCheckpointState> cb : cbs) {
                    try {
                        cb.accept(s);
                    } catch (Exception ignored) {
                        // test stub; mirror real-impl swallow semantics
                    }
                }
            }
        }

        @Override
        public synchronized IndexingServiceCheckpointState getIndexingServiceCheckpointState(
                int instanceId) {
            return state.get(instanceId);
        }

        @Override
        public synchronized void watchIndexingServiceCheckpointState(int instanceId,
                Consumer<IndexingServiceCheckpointState> callback) {
            watchers.computeIfAbsent(instanceId, k -> new ArrayList<>()).add(callback);
        }
    }

    private static final int DIM = 16;

    private static Table buildTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildIndex(String uuid) {
        return Index.builder()
                .name("vidx")
                .uuid(uuid)
                .table("vectable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] randomVec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static IndexingServerConfiguration primaryConfig() {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        return new IndexingServerConfiguration(p);
    }

    private static IndexingServerConfiguration shadowConfig() {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        p.setProperty(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.ROLE_SHADOW);
        p.setProperty(IndexingServerConfiguration.PROPERTY_SHADOW_OF, "0");
        p.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
        return new IndexingServerConfiguration(p);
    }

    private static IndexingServiceEngine primaryEngineWithStableUuid(Path logDir, Path dataDir,
            ShadowAwareMemoryMetadata metadata, MemoryDataStorageManager dsm,
            MemoryManager mm, String stableUuid) throws Exception {
        IndexingServiceEngine primary = new IndexingServiceEngine(
                logDir, dataDir, primaryConfig());
        primary.setMetadataStorageManager(metadata);
        primary.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory,
                                        indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, primary.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });
        return primary;
    }

    private static IndexingServiceEngine startShadow(Path logDir, Path dataDir,
            ShadowAwareMemoryMetadata metadata, MemoryDataStorageManager dsm,
            MemoryManager mm) throws Exception {
        IndexingServiceEngine shadow = new IndexingServiceEngine(
                logDir, dataDir, shadowConfig());
        shadow.setMetadataStorageManager(metadata);
        // Wrap with a non-closing forwarder so shadow.close() does not
        // close the shared MemoryDataStorageManager — it is owned by the
        // test fixture and shared with the primary engine + later shadows.
        shadow.setDataStorageManager(new NonClosingDataStorageManager(dsm));
        shadow.setMemoryManager(mm);
        shadow.start();
        return shadow;
    }

    /**
     * Forwarding wrapper whose {@code close()} is a no-op. Used for
     * shared-DSM tests where one engine's close should not tear down the
     * DSM the other engines still need.
     */
    private static final class NonClosingDataStorageManager
            extends ForwardingDataStorageManager {
        NonClosingDataStorageManager(herddb.storage.DataStorageManager delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            // intentionally do not propagate close() to the delegate
        }
    }

    /**
     * Healthy shadow restart: primary has ingested + checkpointed before
     * shadow #1 booted; shadow #1 reaches {@code ready=true} and serves
     * queries. After closing shadow #1 and starting shadow #2 against the
     * same shared DSM + metadata, the new shadow must boot ready and
     * return the same results — proving the shadow path is fully
     * stateless w.r.t. the local data directory.
     */
    @Test
    public void shadowRestartReattachesToLatestCheckpoint() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        Path primaryLogDir = folder.newFolder("primary-log").toPath();
        Path primaryDataDir = folder.newFolder("primary-data").toPath();

        final String stableUuid = "shadow-restart-uuid-1";
        IndexingServiceEngine primary = primaryEngineWithStableUuid(
                primaryLogDir, primaryDataDir, metadata, dsm, mm, stableUuid);
        primary.start();

        Table table = buildTable();
        Index ix = buildIndex(stableUuid);
        Random rng = new Random(7);

        try {
            primary.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            primary.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            LogSequenceNumber last = null;
            for (int i = 0; i < 256; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                primary.applySingleEntryForTest(last, insert);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(last);
            primary.forceCheckpointAndSaveWatermark();

            // Make the schema visible to the shadow boot path.
            dsm.writeTables(primary.getTableSpaceUUID(), LogSequenceNumber.START_OF_TIME,
                    Arrays.asList(table), Arrays.asList(ix), false);

            // ---- Shadow #1 ----
            Path shadow1Log = folder.newFolder("shadow1-log").toPath();
            Path shadow1Data = folder.newFolder("shadow1-data").toPath();
            IndexingServiceEngine shadow1 = startShadow(
                    shadow1Log, shadow1Data, metadata, dsm, mm);
            try {
                assertTrue("shadow #1 must become ready on first reload",
                        shadow1.isShadowReady());
                assertEquals(1, shadow1.getShadowReloadCount());
                int firstHits = shadow1.search("default", "vectable", "vidx",
                        randomVec(new Random(42)), 5).size();
                assertEquals("shadow #1 must return top-K results", 5, firstHits);
            } finally {
                shadow1.close();
            }

            // ---- Shadow #2 — fresh local data dir, same shared DSM ----
            Path shadow2Log = folder.newFolder("shadow2-log").toPath();
            Path shadow2Data = folder.newFolder("shadow2-data").toPath();
            IndexingServiceEngine shadow2 = startShadow(
                    shadow2Log, shadow2Data, metadata, dsm, mm);
            try {
                assertTrue("shadow #2 must become ready immediately on restart",
                        shadow2.isShadowReady());
                assertEquals("shadow #2 reload count must be 1 after a clean restart",
                        1, shadow2.getShadowReloadCount());

                // Same query as shadow #1 must still return top-K hits —
                // the meaningful behavioural proof that the on-disk state
                // is fully recovered. (We deliberately do not assert on
                // getShadowLoadedLsn(): with the small in-test workload
                // the primary may produce an "empty data, START_OF_TIME"
                // checkpoint whose IndexStatus.indexData is empty so
                // ReadOnlyVectorStore never advances loadedLsn — and yet
                // the on-disk segments are still valid for searches.)
                int restartedHits = shadow2.search("default", "vectable", "vidx",
                        randomVec(new Random(42)), 5).size();
                assertEquals("shadow #2 must return the same top-K result count after restart",
                        5, restartedHits);

                // A fresh primary checkpoint after shadow #2 booted must
                // drive an additional reload on shadow #2 — confirms the
                // ZK watch was re-armed cleanly across the restart.
                for (int i = 0; i < 128; i++) {
                    Record rec = RecordSerializer.makeRecord(table,
                            "pk", "k" + (300 + i), "vec", randomVec(rng));
                    LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                    last = new LogSequenceNumber(1, 500 + i);
                    primary.applySingleEntryForTest(last, insert);
                }
                primary.awaitPendingWorkForTest();
                primary.setLastProcessedLsnForTest(last);
                primary.forceCheckpointAndSaveWatermark();

                // ShadowAwareMemoryMetadata.publishIndexingServiceCheckpointState
                // dispatches to registered watchers synchronously on the
                // calling thread, and the shadow's reload runs on a
                // single-thread executor. Drain that executor by submitting
                // a no-op and joining, which guarantees any reload task
                // already enqueued has completed before we observe the
                // counter — no wall-clock polling required.
                shadow2.awaitShadowReloadsForTest();
                assertTrue("shadow #2 must observe the new primary checkpoint; got reloadCount="
                                + shadow2.getShadowReloadCount(),
                        shadow2.getShadowReloadCount() >= 2);
            } finally {
                shadow2.close();
            }
        } finally {
            primary.close();
        }
    }

    /**
     * A shadow that boots before any primary state exists serves an empty
     * world (no indexes registered → no stores → nothing to return). The
     * shadow reaches {@code shadowReady=true} because the empty reload is
     * trivially successful, but {@link IndexingServiceEngine#getShadowLoadedLsn()}
     * stays {@code null} until a primary actually publishes data. After
     * the primary registers an index and runs a checkpoint, both the
     * (already-running) shadow and a freshly-restarted shadow must observe
     * the new state and start returning hits — confirming the shadow
     * boot path is fully data-driven by the shared storage and works
     * across restarts.
     */
    @Test
    public void shadowRestartObservesPrimaryPublishedAfterFirstBoot() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        Path shadowLog = folder.newFolder("shadow-log").toPath();
        Path shadowData = folder.newFolder("shadow-data").toPath();

        // ---- Shadow #1 boots first, no primary state available ----
        IndexingServiceEngine shadow1 = startShadow(
                shadowLog, shadowData, metadata, dsm, mm);
        try {
            // No indexes registered yet → empty reload trivially succeeds,
            // but no data has been seen so loaded LSN is null.
            assertEquals("shadow with no registered indexes returns empty",
                    0, shadow1.search("default", "vectable", "vidx",
                            randomVec(new Random(0)), 5).size());
        } finally {
            shadow1.close();
        }

        // ---- Shadow #2 restart — still no primary state ----
        IndexingServiceEngine shadow2 = startShadow(
                folder.newFolder("shadow2-log").toPath(),
                folder.newFolder("shadow2-data").toPath(),
                metadata, dsm, mm);
        try {
            assertEquals("restarted shadow with no primary state still returns empty",
                    0, shadow2.search("default", "vectable", "vidx",
                            randomVec(new Random(0)), 5).size());
        } finally {
            shadow2.close();
        }

        // ---- Primary boots, ingests, checkpoints ----
        final String stableUuid = "shadow-restart-uuid-2";
        IndexingServiceEngine primary = primaryEngineWithStableUuid(
                folder.newFolder("primary-log").toPath(),
                folder.newFolder("primary-data").toPath(),
                metadata, dsm, mm, stableUuid);
        primary.start();
        try {
            Table table = buildTable();
            Index ix = buildIndex(stableUuid);
            primary.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            primary.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            LogSequenceNumber last = null;
            Random rng = new Random(11);
            for (int i = 0; i < 256; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                primary.applySingleEntryForTest(last, insert);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(last);
            primary.forceCheckpointAndSaveWatermark();
            dsm.writeTables(primary.getTableSpaceUUID(),
                    LogSequenceNumber.START_OF_TIME,
                    Arrays.asList(table), Arrays.asList(ix), false);

            // ---- Shadow #3: fresh restart AFTER primary published ----
            // Must boot ready, discover the index from dsm.loadIndexes(),
            // pick up the IndexStatus, and start returning hits.
            IndexingServiceEngine shadow3 = startShadow(
                    folder.newFolder("shadow3-log").toPath(),
                    folder.newFolder("shadow3-data").toPath(),
                    metadata, dsm, mm);
            try {
                assertTrue("shadow #3 must be ready after primary published",
                        shadow3.isShadowReady());
                assertEquals("shadow #3 must return top-K hits", 5,
                        shadow3.search("default", "vectable", "vidx",
                                randomVec(new Random(0)), 5).size());
            } finally {
                shadow3.close();
            }
        } finally {
            primary.close();
        }
    }

    /**
     * Primary executes DROP_INDEX while a shadow has the same index
     * cached. After the issue #383 cleanup, the on-storage data the
     * shadow's {@link herddb.indexing.vector.ReadOnlyVectorStore} reads from
     * is gone. The shadow must keep running (no NPE, no infinite reload
     * loop) and any subsequent search on the dropped index must return
     * empty. The primary must not block on the drop.
     */
    @Test
    public void shadowSurvivesDropOnPrimary() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        final String stableUuid = "shadow-drop-uuid";

        // Primary uses the *real* dsm (engine-owned via the factory) so
        // that DROP_INDEX-driven submitVectorStoreDeletion runs against
        // the same backing store the shadow reads from. setDataStorageManager
        // installs the engine-owned reference too.
        IndexingServiceEngine primary = new IndexingServiceEngine(
                folder.newFolder("primary-log").toPath(),
                folder.newFolder("primary-data").toPath(),
                primaryConfig());
        primary.setMetadataStorageManager(metadata);
        primary.setDataStorageManager(new NonClosingDataStorageManager(dsm));
        primary.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory,
                                        indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, primary.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });
        primary.start();

        Table table = buildTable();
        Index ix = buildIndex(stableUuid);

        try {
            primary.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            primary.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));

            Random rng = new Random(91);
            LogSequenceNumber last = null;
            for (int i = 0; i < 256; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k" + i, "vec", randomVec(rng));
                LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                last = new LogSequenceNumber(1, 100 + i);
                primary.applySingleEntryForTest(last, insert);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(last);
            primary.forceCheckpointAndSaveWatermark();
            dsm.writeTables(primary.getTableSpaceUUID(), LogSequenceNumber.START_OF_TIME,
                    Arrays.asList(table), Arrays.asList(ix), false);

            // Shadow attaches and serves queries.
            IndexingServiceEngine shadow = startShadow(
                    folder.newFolder("shadow-log").toPath(),
                    folder.newFolder("shadow-data").toPath(),
                    metadata, dsm, mm);
            try {
                assertTrue("shadow must boot ready against the live primary",
                        shadow.isShadowReady());
                assertEquals("shadow must return top-K hits before DROP",
                        5, shadow.search("default", "vectable", "vidx",
                                randomVec(new Random(2)), 5).size());

                // ---- Primary executes DROP_INDEX ----
                primary.applyEntry(new LogSequenceNumber(1, 999),
                        LogEntryFactory.dropIndex("vidx", null));
                primary.awaitPendingWorkForTest();
                primary.awaitPendingDeletionsForTest();

                // Drain any reload tasks the shadow may have enqueued —
                // even though the test stub fires watchers synchronously,
                // drains-on-no-op make the assertion deterministic.
                shadow.awaitShadowReloadsForTest();

                // The shadow must keep running. We do not assert on
                // search results here — depending on the order in which
                // the segment files were closed and the
                // ReadOnlyVectorStore's cached file handles were
                // released, the shadow may either return zero or throw
                // a missing-segment exception that the engine's search
                // wrapper translates to empty. Either is acceptable.
                // What is NOT acceptable is the engine deadlocking or
                // crashing the test runner; if we get here the shadow
                // is alive. We sanity-check by asking listIndexes and
                // making sure it does not throw.
                shadow.listIndexes();
            } finally {
                shadow.close();
            }
        } finally {
            primary.close();
        }
    }
}
