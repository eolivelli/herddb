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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.indexing.vector.AbstractVectorStore;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.indexing.vector.ReadOnlyVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #617 + pr-reviewer follow-ups #1 and #2: engine-level edge cases for
 * {@link IndexingServiceEngine#deleteSegment} that the CLI/gRPC tests in
 * {@code herddb.indexing.admin.IndexingAdminCliDeleteSegmentTest} and the
 * full E2E test in {@link ShadowDeleteSegmentE2ETest} do not cover.
 *
 * <ol>
 *   <li><b>Race-path test (follow-up #1)</b>: drive the engine into the
 *       branch where {@link PersistentVectorStore#dropSegmentByStorageKey}
 *       returns {@code removed=false} <em>after</em> the engine has already
 *       snapshotted the storage key. We inject a parallel drop via a
 *       test-only hook that fires inside {@code deleteSegment} between the
 *       engine's snapshot/probe and the engine's own drop call. Assert that
 *       the engine reports the documented sentinel:
 *       {@code vectorsLost == -1L && removed == false}.</li>
 *   <li><b>Shadow / read-only rejection (follow-up #2)</b>: install a
 *       {@link ReadOnlyVectorStore} directly into the engine's
 *       {@code vectorStores} map (bypassing the gRPC layer entirely) and
 *       call {@link IndexingServiceEngine#deleteSegment} on it. The
 *       production gate at
 *       {@link IndexingServiceImpl#deleteSegment(herddb.indexing.proto.DeleteSegmentRequest,
 *       io.grpc.stub.StreamObserver)} short-circuits these RPCs at the
 *       service boundary, so this engine-level belt-and-braces refusal is
 *       reachable only by tests that drive the engine directly. The
 *       exception message must call out both "shadow replica" and "primary"
 *       so an operator who somehow reaches this branch (e.g. a future
 *       caller that bypasses the gRPC layer) gets actionable diagnostics.
 * </ol>
 */
public class IndexingServiceEngineDeleteSegmentEdgeCasesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;

    @Before
    public void setUp() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
    }

    private static Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace(TableSpace.DEFAULT)
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index createIndex(String uuid) {
        return Index.builder()
                .name("vidx").uuid(uuid)
                .table("vectable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private IndexingServiceEngine newEngine(MemoryDataStorageManager dsm, MemoryManager mm,
                                            final String stableUuid,
                                            final AtomicReference<PersistentVectorStore> storeRef) throws Exception {
        Path logDir = folder.newFolder().toPath();
        Path dataDir = folder.newFolder().toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME, TableSpace.DEFAULT);
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        final IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);

        herddb.mem.MemoryMetadataStorageManager memMeta = new herddb.mem.MemoryMetadataStorageManager();
        memMeta.start();
        memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(memMeta);
        engine.setDataStorageManager(dsm);
        engine.setMemoryManager(mm);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirArg, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDirArg, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (storeRef != null) {
                storeRef.set(pvs);
            }
            return pvs;
        });
        return engine;
    }

    /**
     * pr-reviewer follow-up #1: race path between the engine's snapshot and
     * the engine's drop. The pre-drop hook removes the same segment from a
     * parallel actor (here a direct {@code dropSegmentByStorageKey} call on
     * the store, simulating a concurrent compaction swap that finished
     * between the engine's two reads). The engine's own drop then sees the
     * segment gone and must return the documented race-sentinel.
     */
    @Test(timeout = 60_000)
    public void engineReportsRaceSentinelWhenSegmentDroppedConcurrently() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-617-race";

        AtomicReference<PersistentVectorStore> primaryStore = new AtomicReference<>();
        IndexingServiceEngine engine = newEngine(dsm, mm, stableUuid, primaryStore);
        engine.start();
        try {
            Table t = createTable();
            Index idx = createIndex(stableUuid);
            engine.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

            Random rng = new Random(617);
            int dim = 12;
            LogSequenceNumber last = null;
            // Two checkpoints → two segments, so even after the "concurrent"
            // drop the snapshot list (taken before the hook) still contained
            // the victim — without that, the up-front presence-check inside
            // deleteSegment would throw DeleteSegmentException ("not
            // registered") and we would never reach the race branch.
            for (int batch = 0; batch < 2; batch++) {
                for (int i = 0; i < 40; i++) {
                    Record r = RecordSerializer.makeRecord(t,
                            "pk", "k" + batch + "_" + i, "vec", randomVector(rng, dim));
                    LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
                    last = new LogSequenceNumber(1, 100 + batch * 100 + i);
                    engine.applySingleEntryForTest(last, ins);
                }
                engine.awaitPendingWorkForTest();
                engine.setLastProcessedLsnForTest(last);
                engine.forceCheckpointAndSaveWatermark();
            }

            final PersistentVectorStore pvs = primaryStore.get();
            assertNotNull("engine must have created the persistent store", pvs);
            List<String> keys = pvs.getSegmentStorageKeysSnapshot();
            assertTrue("test setup must produce ≥2 segments, got " + keys.size(),
                    keys.size() >= 2);
            final String victim = keys.get(0);

            // Pre-drop race hook: fires inside engine.deleteSegment AFTER the
            // snapshot+probe but BEFORE the engine's own drop. Removes the
            // victim out from under the engine. The hook itself does NOT use
            // a separate thread (the deferred-close protocol around
            // dropSegmentByStorageKey is reentrant-safe) — the important
            // invariant is just that the segment is gone by the time the
            // engine's drop runs.
            final AtomicReference<AbstractVectorStore.SegmentDropResult> sideDropRef =
                    new AtomicReference<>();
            engine.setPreDropRaceHookForTests(() -> {
                AbstractVectorStore.SegmentDropResult r = pvs.dropSegmentByStorageKey(victim);
                sideDropRef.set(r);
            });

            IndexingServiceEngine.DeleteSegmentResult result = engine.deleteSegment(
                    "vectable", "vidx", victim,
                    /* purgeStorage */ false, /* force */ true);

            // Pre-condition: the side-channel drop must have actually
            // removed the segment, otherwise the race branch would not be
            // exercised.
            AbstractVectorStore.SegmentDropResult sideDrop = sideDropRef.get();
            assertNotNull("pre-drop hook must have fired", sideDrop);
            assertTrue("pre-drop hook must have removed the segment",
                    sideDrop.removed);

            // The race-path documented contract: removed=false AND
            // vectorsLost == -1L (sentinel for "unknown — cannot compute the
            // count because the segment handle is no longer accessible").
            assertFalse("engine must report removed=false on the race path",
                    result.removed);
            assertEquals("engine must surface vectors_lost=-1 on the race path",
                    -1L, result.vectorsLost);
            assertEquals("response segment name must match the request",
                    victim, result.segment);
            assertFalse("storage_purged must be false on the race path "
                            + "(engine bails out before touching storage)",
                    result.storagePurged);

            // And the segment is gone from the active list (the side-channel
            // drop removed it).
            assertFalse("victim must be gone from the active segment list",
                    pvs.getSegmentStorageKeysSnapshot().contains(victim));
        } finally {
            engine.setPreDropRaceHookForTests(null);
            engine.close();
        }
    }

    /**
     * pr-reviewer follow-up #2: engine-only refusal when the configured
     * store is a {@link ReadOnlyVectorStore}. The production
     * {@code IndexingServiceImpl.deleteSegment} short-circuits these RPCs
     * with a {@code FAILED_PRECONDITION} BEFORE reaching the engine — this
     * test bypasses that gate by calling {@code engine.deleteSegment(...)}
     * directly to prove the engine's belt-and-braces guard is in place.
     *
     * <p>The exception message must mention both "shadow replica" and
     * "primary" so a future caller that bypasses the gRPC layer still gets
     * an actionable diagnostic.
     */
    @Test(timeout = 30_000)
    public void engineRefusesDeleteOnReadOnlyVectorStoreWithDiagnosticMessage() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-617-readonly";

        IndexingServiceEngine engine = newEngine(dsm, mm, stableUuid, null);
        engine.start();
        ReadOnlyVectorStore readOnly = null;
        try {
            Table t = createTable();
            Index idx = createIndex(stableUuid);
            // Schema must exist so the engine's schema-tracker accepts the
            // synthetic ReadOnlyVectorStore registration.
            engine.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
            engine.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

            // Build a real ReadOnlyVectorStore (no segments, no data — the
            // delete-segment refusal must fire on the type check alone, and
            // must not depend on the store having any state). Use a
            // dedicated tmp dir so the underlying PersistentVectorStore
            // delegate does not collide with the in-flight production
            // store.
            Path roTmp = folder.newFolder("ro-store").toPath();
            readOnly = new ReadOnlyVectorStore(
                    "vidx", "vectable", engine.getTableSpaceUUID(),
                    "vec", stableUuid + "-ro", roTmp,
                    dsm, mm,
                    16, 100, 1.2f, 1.4f,
                    /* fusedPQ */ true,
                    /* maxSegmentSize */ 2_000_000_000L,
                    /* maxLiveGraphSize */ 0,
                    VectorSimilarityFunction.EUCLIDEAN);
            readOnly.start();

            // Replace the (PersistentVectorStore) the factory just installed
            // with the synthetic ReadOnlyVectorStore — this is the precise
            // scenario the engine guard is meant to catch.
            engine.registerIndexForTest(idx, readOnly);

            try {
                engine.deleteSegment("vectable", "vidx", "any-segment-key",
                        /* purgeStorage */ false, /* force */ true);
                fail("engine must refuse deleteSegment on a ReadOnlyVectorStore");
            } catch (IndexingServiceEngine.DeleteSegmentException expected) {
                String msg = expected.getMessage();
                assertNotNull(msg);
                assertTrue("refusal message must mention 'shadow replica', got: " + msg,
                        msg.contains("shadow replica"));
                assertTrue("refusal message must mention 'primary' (so the operator"
                                + " knows where to retry), got: " + msg,
                        msg.contains("primary"));
            }
        } finally {
            if (readOnly != null) {
                try {
                    readOnly.close();
                } catch (Exception ignore) {
                    // best-effort teardown
                }
            }
            engine.close();
        }
    }
}
