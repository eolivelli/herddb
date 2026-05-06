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
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.log.IndexingServiceRebalanceDescriptor;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the indexing-service engine's behaviour across restarts of an
 * instance that is in {@link IndexingServiceEngine.EngineStatus#JOINING}
 * during a scale-up — the third scenario asked for by issue #383.
 *
 * <p>A pod started with
 * {@link IndexingServerConfiguration#PROPERTY_BOOTSTRAP_FROM_REBALANCE
 * bootstrap.from.rebalance}=true boots in {@code JOINING} and drops every
 * commit-log entry until a {@code REBALANCE} arrives. The tests below cover
 * three distinct restart points around that lifecycle:
 *
 * <ol>
 *   <li>Restart <em>before</em> the first REBALANCE — pod is killed mid
 *       JOIN; on restart it must remain {@code JOINING} and must not
 *       silently drop any DML or fall through to {@code ACTIVE}.</li>
 *   <li>Restart <em>after</em> the first REBALANCE has been observed and
 *       a checkpoint has saved the resulting schema — pod is killed after
 *       reaching ACTIVE; on restart, with a non-empty watermark snapshot
 *       in place, it must boot directly into ACTIVE without re-doing the
 *       JOINING dance.</li>
 *   <li>Restart with a <em>stale pre-REBALANCE</em> persisted state — the
 *       pod was killed after taking some pre-REBALANCE entries and persisting
 *       partial routing data; on restart with
 *       {@code bootstrap.from.rebalance=true} again, it must re-enter
 *       {@code JOINING} and ignore the stale state until a fresh REBALANCE
 *       arrives.</li>
 * </ol>
 */
public class JoiningRestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int VECTOR_DIM = 3;

    private static Table buildTable() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex(int numShards) {
        return Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .build();
    }

    private static MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private static IndexingServerConfiguration joiningConfig(int instanceId, int numInstances) {
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

    private static IndexingServerConfiguration nonJoiningConfig(int instanceId, int numInstances) {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstances));
        return new IndexingServerConfiguration(props);
    }

    /**
     * Watermark store that retains its content across in-process engine
     * restarts. {@code load()} returns whatever {@code save()} most recently
     * received — exactly the on-disk semantics the engine relies on. Tests
     * use a single instance for both the pre-restart and post-restart
     * engines so the second engine "loads" what the first one "saved".
     */
    private static final class InMemoryWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> current =
                new AtomicReference<>(WatermarkSnapshot.START_OF_TIME);

        @Override
        public WatermarkSnapshot load() {
            return current.get();
        }

        @Override
        public void save(WatermarkSnapshot snapshot) throws IOException {
            current.set(snapshot);
        }
    }

    /**
     * The pod is killed after taking a few pre-REBALANCE entries (which the
     * JOINING engine drops) but BEFORE any REBALANCE arrives. On restart,
     * with the same {@code bootstrap.from.rebalance=true} configuration and
     * an empty watermark, it must re-enter {@code JOINING} and continue to
     * drop entries. Once a REBALANCE finally arrives, the engine transitions
     * to ACTIVE exactly as if the restart had never happened.
     */
    @Test
    public void joiningRestartStaysJoiningUntilRebalance() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        InMemoryWatermarkStore watermark = new InMemoryWatermarkStore();
        MemoryMetadataStorageManager meta = newMeta();

        // ---- Pod #1: boot JOINING, take some entries, kill before REBALANCE ----
        IndexingServiceEngine engine1 = new IndexingServiceEngine(
                logDir, dataDir, joiningConfig(0, 1));
        engine1.setMetadataStorageManager(meta);
        engine1.setWatermarkStore(watermark);
        try {
            engine1.start();
            assertEquals("pod #1 must boot JOINING with bootstrap.from.rebalance=true",
                    IndexingServiceEngine.EngineStatus.JOINING,
                    engine1.getEngineStatus());

            // Pre-REBALANCE entries — all dropped while JOINING.
            Table table = buildTable();
            engine1.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            for (int i = 0; i < 4; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "lost_" + i, "vec", new float[]{i, i, i});
                engine1.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine1.awaitPendingWorkForTest();

            assertEquals("pod #1 must still be JOINING — no REBALANCE seen yet",
                    IndexingServiceEngine.EngineStatus.JOINING,
                    engine1.getEngineStatus());
        } finally {
            engine1.close();
        }

        // The watermark must still be START_OF_TIME (no checkpoint happened
        // because the engine never reached ACTIVE).
        assertEquals("a JOINING engine never advances the watermark",
                WatermarkSnapshot.START_OF_TIME, watermark.load());

        // ---- Pod #2: same config, same dataDir, same watermark store ----
        IndexingServiceEngine engine2 = new IndexingServiceEngine(
                logDir, dataDir, joiningConfig(0, 1));
        engine2.setMetadataStorageManager(meta);
        engine2.setWatermarkStore(watermark);
        try {
            engine2.start();
            assertEquals("pod #2 must remain JOINING after restart with same bootstrap-from-rebalance flag",
                    IndexingServiceEngine.EngineStatus.JOINING,
                    engine2.getEngineStatus());

            // Pre-REBALANCE entries are still dropped on the restarted pod.
            Table table = buildTable();
            for (int i = 0; i < 3; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "lost2_" + i, "vec", new float[]{i, i, i});
                engine2.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine2.awaitPendingWorkForTest();
            assertEquals("DML before REBALANCE is dropped on the restarted pod too",
                    IndexingServiceEngine.EngineStatus.JOINING,
                    engine2.getEngineStatus());

            // Now the REBALANCE that was always missing arrives.
            Index ix = buildVectorIndex(2);
            IndexingServiceRebalanceDescriptor descriptor =
                    new IndexingServiceRebalanceDescriptor(
                            500L, 1,
                            Collections.singletonList(table),
                            Collections.singletonList(ix));
            engine2.applySingleEntryForTest(new LogSequenceNumber(2, 1),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            engine2.awaitPendingWorkForTest();

            assertEquals("REBALANCE must lift the engine out of JOINING",
                    IndexingServiceEngine.EngineStatus.ACTIVE,
                    engine2.getEngineStatus());
            assertNotNull("descriptor must be recorded after JOIN bootstrap on restart",
                    engine2.getLastObservedRebalance());
            assertEquals(500L, engine2.getObservedRebalanceEpoch());

            // Post-REBALANCE inserts land normally.
            for (int i = 0; i < 5; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "kept_" + i, "vec", new float[]{i, i, i});
                engine2.applySingleEntryForTest(new LogSequenceNumber(2, 100 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine2.awaitPendingWorkForTest();
            assertEquals("post-REBALANCE inserts must land in the index after a JOINING restart",
                    5, engine2.search("default", "mytable", "vidx",
                            new float[]{0, 0, 0}, 100).size());
        } finally {
            engine2.close();
        }
    }

    /**
     * The pod completes its JOIN, runs a successful checkpoint that
     * persists the schema in the watermark snapshot, then is killed and
     * restarted with the regular (non-JOINING) configuration. With the
     * snapshot in place the engine boots directly {@code ACTIVE} — there
     * is no need to wait for another REBALANCE — and queries continue to
     * work without re-replaying the pre-checkpoint history.
     */
    @Test
    public void joiningRestartAfterFirstRebalanceIsActive() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        InMemoryWatermarkStore watermark = new InMemoryWatermarkStore();
        MemoryMetadataStorageManager meta = newMeta();

        Table table = buildTable();
        Index ix = buildVectorIndex(2);

        // ---- Pod #1: JOINING → REBALANCE → ACTIVE → ingest → checkpoint ----
        IndexingServiceEngine engine1 = new IndexingServiceEngine(
                logDir, dataDir, joiningConfig(0, 1));
        engine1.setMetadataStorageManager(meta);
        engine1.setWatermarkStore(watermark);
        try {
            engine1.start();

            IndexingServiceRebalanceDescriptor descriptor =
                    new IndexingServiceRebalanceDescriptor(
                            300L, 1,
                            Collections.singletonList(table),
                            Collections.singletonList(ix));
            engine1.applySingleEntryForTest(new LogSequenceNumber(1, 100),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            engine1.awaitPendingWorkForTest();
            assertEquals(IndexingServiceEngine.EngineStatus.ACTIVE,
                    engine1.getEngineStatus());

            // Ingest some vectors and checkpoint to capture them in the snapshot.
            LogSequenceNumber last = null;
            for (int i = 0; i < 7; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k_" + i, "vec", new float[]{i, i, i});
                last = new LogSequenceNumber(1, 200 + i);
                engine1.applySingleEntryForTest(last,
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine1.awaitPendingWorkForTest();
            engine1.setLastProcessedLsnForTest(last);
            engine1.forceCheckpointAndSaveWatermark();
        } finally {
            engine1.close();
        }

        // The watermark snapshot must now carry schema (issue #368).
        WatermarkSnapshot saved = watermark.load();
        assertTrue("snapshot must carry schema after JOIN+ACTIVE+checkpoint",
                saved.hasSchema());
        assertEquals(1, saved.tables.size());
        assertEquals(1, saved.vectorIndexes.size());

        // ---- Pod #2: same dataDir + same watermark, default config ----
        // Once the engine has reached ACTIVE and persisted a snapshot, a
        // restart should NOT re-arm the JOINING fallback — the saved
        // snapshot already provides the schema. Booting directly into
        // ACTIVE is critical: a JOINING-on-restart engine would silently
        // drop every DML it sees until the next REBALANCE arrives.
        IndexingServiceEngine engine2 = new IndexingServiceEngine(
                logDir, dataDir, nonJoiningConfig(0, 1));
        engine2.setMetadataStorageManager(meta);
        engine2.setWatermarkStore(watermark);
        try {
            engine2.start();
            assertEquals("post-checkpoint restart with normal config must boot ACTIVE",
                    IndexingServiceEngine.EngineStatus.ACTIVE,
                    engine2.getEngineStatus());
            assertEquals("snapshot must restore the index",
                    1, engine2.listIndexes().size());

            // Post-restart inserts (LSNs after the saved watermark) land
            // normally — the engine resumed cleanly.
            for (int i = 0; i < 4; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "post_" + i, "vec", new float[]{i, i, i});
                engine2.applySingleEntryForTest(
                        new LogSequenceNumber(2, 1 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine2.awaitPendingWorkForTest();
            // The 7 pre-restart vectors are in the in-memory store on engine1
            // but NOT on engine2 (in-memory storage is per-engine), and the
            // 4 post-restart vectors land in engine2's store. So the search
            // returns exactly 4.
            assertEquals("post-restart inserts must be searchable",
                    4, engine2.search("default", "mytable", "vidx",
                            new float[]{0, 0, 0}, 100).size());
        } finally {
            engine2.close();
        }
    }

    /**
     * Restart with {@code bootstrap.from.rebalance=true} again, even though
     * the watermark store somehow carries a stale schema. The engine MUST
     * still re-enter {@code JOINING} (the boot flag wins) and ignore stale
     * schema information until a fresh REBALANCE arrives. This covers the
     * scale-up scenario where a pod is repeatedly rolled while
     * {@code bootstrap.from.rebalance=true} is still set in its config.
     */
    @Test
    public void joiningRestartWithStalePreRebalanceWatermark() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // Pre-bake a stale snapshot — looks like a previous run of the same
        // pod once managed to save schema, but then it was hard-reset and
        // is now coming up as a fresh JOINING pod.
        InMemoryWatermarkStore watermark = new InMemoryWatermarkStore();
        watermark.save(new WatermarkSnapshot(new LogSequenceNumber(99, 99), 1, 0L,
                Collections.singletonList(buildTable()),
                Collections.singletonList(buildVectorIndex(2))));

        MemoryMetadataStorageManager meta = newMeta();
        IndexingServiceEngine engine = new IndexingServiceEngine(
                logDir, dataDir, joiningConfig(0, 1));
        engine.setMetadataStorageManager(meta);
        engine.setWatermarkStore(watermark);
        try {
            engine.start();
            // Even though the watermark has schema, bootstrap-from-rebalance
            // overrides it — the engine is JOINING.
            assertEquals("bootstrap.from.rebalance=true must beat any pre-existing snapshot",
                    IndexingServiceEngine.EngineStatus.JOINING,
                    engine.getEngineStatus());

            // Sanity: DML is dropped while JOINING even though the schema
            // is technically known from the snapshot.
            Table table = buildTable();
            for (int i = 0; i < 3; i++) {
                Record rec = RecordSerializer.makeRecord(table,
                        "pk", "k_" + i, "vec", new float[]{i, i, i});
                engine.applySingleEntryForTest(new LogSequenceNumber(100, 1 + i),
                        LogEntryFactory.insert(table, rec.key, rec.value, null));
            }
            engine.awaitPendingWorkForTest();
            assertEquals("the JOINING gate must drop every DML, even with schema in the snapshot",
                    0, engine.search("default", "mytable", "vidx",
                            new float[]{0, 0, 0}, 100).size());
        } finally {
            engine.close();
        }
    }
}
