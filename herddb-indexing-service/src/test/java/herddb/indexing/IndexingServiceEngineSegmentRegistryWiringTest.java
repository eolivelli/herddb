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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.PersistentVectorStore;
import herddb.indexing.segment.SegmentAssignmentWatcher;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Engine-level wiring coverage for issue #491: when
 * {@code indexing.optimizer.enabled=true} is set on a cluster-mode IS, the
 * production {@code vectorStoreFactory} must attach a
 * {@code SegmentRegistryPublisher} to every {@link PersistentVectorStore} it
 * creates so that each successful checkpoint publishes its sealed segments to
 * the ZK segment registry — making them visible to the external index-optimizer
 * service.
 *
 * <p>The bug this guards against: the production lambda used to ignore the
 * property and never call {@code setSegmentPublisher} / {@code
 * setExternalCompactionEnabled}, so the optimizer pod's
 * {@code IndexOptimizerEngine.runOnce()} consistently observed zero indexes
 * (see issue #491 root-cause analysis).
 *
 * <p>Three scenarios are covered:
 * <ol>
 *   <li>Cluster mode + {@code optimizer.enabled=true}: registry wired,
 *       publisher attached, external compaction enabled, ZK populated after
 *       checkpoint.</li>
 *   <li>Cluster mode + {@code optimizer.enabled} unset (legacy): registry
 *       null, no publisher attached, ZK stays empty after checkpoint.</li>
 *   <li>Standalone mode + {@code optimizer.enabled=true} (misconfiguration):
 *       registry null, WARNING logged, engine still starts so existing
 *       deployments don't regress.</li>
 * </ol>
 *
 * <p>Pattern: TestingServer ZK directly (not {@code ZKTestEnv}) so this stays
 * a non-cluster test under the IS module's surefire config — same convention
 * as {@link MixedModeIndexesTest} and
 * {@link herddb.indexing.segment.SegmentRegistryPublisherIntegrationTest}.
 */
public class IndexingServiceEngineSegmentRegistryWiringTest {

    private static final String TABLE_NAME = "vectable";
    private static final String VECTOR_COL = "vec";
    private static final String INDEX_NAME = "vidx";
    /**
     * Number of vectors that triggers the FusedPQ multipart write path —
     * matches the threshold used by
     * {@link PersistentVectorStore#MIN_VECTORS_FOR_FUSED_PQ}. 300 gives a
     * comfortable margin and exercises a single-segment checkpoint emit.
     */
    private static final int NUM_VECTORS = 300;
    private static final int VECTOR_DIM = 32;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TestingServer zkServer;

    /**
     * Saved values of the static checkpoint gates on
     * {@link PersistentVectorStore} so concurrent tests in the same JVM
     * cannot observe drift if a method exits abnormally.
     */
    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        // Drop the per-checkpoint live-vector gate so a 300-vector test
        // ingest produces a real sealed segment (production default is
        // 50 000). The gate is the only reason a small-N test would ever
        // skip publication entirely.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() throws Exception {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
        if (zkServer != null) {
            zkServer.close();
        }
    }

    @Test
    public void optimizerEnabledWithZkMetadataWiresPublisherAndPublishesToRegistry()
            throws Exception {
        Path logDir = tmpFolder.newFolder("log").toPath();
        Path dataDir = tmpFolder.newFolder("data").toPath();
        // Use a per-test ZK base path so concurrent surefire forks don't clash
        // on /herd-test-491-engine. The ZK metadata manager creates the path
        // when start(true) is called below.
        String basePath = "/herd-test-491-engine-on";

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "file");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED, "true");
        // Tighten the tablespace-wait timeout: we ensure the tablespace before
        // start() so the polling loop must succeed on its first iteration.
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS, "10000");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "100");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        try (ZookeeperMetadataStorageManager zkMeta = new ZookeeperMetadataStorageManager(
                zkServer.getConnectString(), 30000, basePath)) {
            zkMeta.start(true /* formatIfNeeded */);
            zkMeta.ensureDefaultTableSpace("local", "local", 0, 1);

            FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
            MemoryManager mm = new MemoryManager(
                    128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

            try (IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config)) {
                engine.setMetadataStorageManager(zkMeta);
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);
                engine.start();

                // (1) After start(), the engine MUST have wired a SegmentRegistryClient
                // because indexing.optimizer.enabled=true and the metadata manager is
                // ZK-backed. Without this, the production factory has no registry to
                // attach a publisher to and the optimizer sees zero indexes.
                SegmentRegistryClient registry = engine.getSegmentRegistry();
                assertNotNull("engine must wire SegmentRegistryClient when "
                        + "indexing.optimizer.enabled=true and metadata is ZK-backed",
                        registry);

                // The registry root must be reachable — ensureRoot() is called inside
                // start() and listSegments under a fresh tablespace must return empty
                // (proves the ZK path was created and the supplier returns a live
                // ZooKeeper handle).
                String tableSpaceUUID = engine.getTableSpaceUUID();
                assertNotNull("tableSpaceUUID must be resolved by start()", tableSpaceUUID);

                // (2) Use the engine's installed (production) factory to create a
                // store. The factory MUST attach a publisher and flip the per-store
                // external-compaction switch. Use a non-empty index property bag so
                // the lambda exercises its similarity-resolution branch.
                AbstractVectorStore store = engine.getVectorStoreFactory().create(
                        INDEX_NAME, TABLE_NAME, VECTOR_COL, dataDir,
                        Collections.singletonMap("similarity", "euclidean"));
                try {
                    assertTrue("storage type=file must produce a PersistentVectorStore, got "
                            + store.getClass(), store instanceof PersistentVectorStore);
                    PersistentVectorStore pvs = (PersistentVectorStore) store;
                    assertTrue("publisher wiring must flip externalCompactionEnabled=true",
                            pvs.isExternalCompactionEnabled());

                    // (3) End-to-end: ingest enough vectors to clear the FusedPQ
                    // gate, run a checkpoint, then verify ZK lists at least one
                    // ACTIVE segment owned by the local instance for this index.
                    String indexUuid = pvs.getIndexUuid();
                    Random rng = new Random(491);
                    for (int i = 0; i < NUM_VECTORS; i++) {
                        pvs.addVector(Bytes.from_int(i), randomVector(rng, VECTOR_DIM));
                    }
                    pvs.checkpoint();

                    List<VersionedSegmentMetadata> registered =
                            registry.listSegments(tableSpaceUUID, indexUuid);
                    assertFalse("checkpoint with publisher attached must register at least"
                            + " one segment in ZK (saw " + registered.size() + ")",
                            registered.isEmpty());
                    long activeCount = 0;
                    long totalVectors = 0;
                    for (VersionedSegmentMetadata v : registered) {
                        SegmentMetadata m = v.metadata();
                        assertEquals(tableSpaceUUID, m.getTablespaceUuid());
                        assertEquals(indexUuid, m.getIndexUuid());
                        assertEquals(TABLE_NAME, m.getTableName());
                        assertEquals(INDEX_NAME, m.getIndexName());
                        if (m.getState() == SegmentState.ACTIVE) {
                            activeCount++;
                            totalVectors += m.getVectorCount();
                        }
                    }
                    assertTrue("at least one segment must reach ACTIVE state, saw "
                            + activeCount, activeCount >= 1);
                    assertEquals("total registered vector count must equal ingested count",
                            (long) NUM_VECTORS, totalVectors);
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test
    public void optimizerDisabledLeavesRegistryUnwiredAndPublishesNothing() throws Exception {
        Path logDir = tmpFolder.newFolder("log").toPath();
        Path dataDir = tmpFolder.newFolder("data").toPath();
        String basePath = "/herd-test-491-engine-off";

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "file");
        // indexing.optimizer.enabled NOT set → defaults to false (legacy).
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS, "10000");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "100");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        try (ZookeeperMetadataStorageManager zkMeta = new ZookeeperMetadataStorageManager(
                zkServer.getConnectString(), 30000, basePath)) {
            zkMeta.start(true);
            zkMeta.ensureDefaultTableSpace("local", "local", 0, 1);

            FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
            MemoryManager mm = new MemoryManager(
                    128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

            try (IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config)) {
                engine.setMetadataStorageManager(zkMeta);
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);
                engine.start();

                // Legacy mode: no registry handle.
                assertNull("engine must NOT wire a registry when "
                        + "indexing.optimizer.enabled is false (default)",
                        engine.getSegmentRegistry());

                AbstractVectorStore store = engine.getVectorStoreFactory().create(
                        INDEX_NAME, TABLE_NAME, VECTOR_COL, dataDir,
                        Collections.singletonMap("similarity", "euclidean"));
                try {
                    PersistentVectorStore pvs = (PersistentVectorStore) store;
                    assertFalse("legacy mode must leave externalCompactionEnabled=false",
                            pvs.isExternalCompactionEnabled());

                    Random rng = new Random(491);
                    for (int i = 0; i < NUM_VECTORS; i++) {
                        pvs.addVector(Bytes.from_int(i), randomVector(rng, VECTOR_DIM));
                    }
                    pvs.checkpoint();

                    // Build a side-channel registry pointed at the same ZK and assert
                    // nothing landed under the engine's tablespace. (We can't use the
                    // engine's registry because there isn't one — that's the point.)
                    SegmentRegistryClient probe = new SegmentRegistryClient(
                            zkMeta::getZooKeeper, basePath);
                    probe.ensureRoot();
                    List<VersionedSegmentMetadata> segs = probe.listSegments(
                            engine.getTableSpaceUUID(), pvs.getIndexUuid());
                    assertTrue("legacy mode must not publish any segment to ZK, saw "
                            + segs.size(), segs.isEmpty());
                } finally {
                    store.close();
                }
            }
        }
    }

    @Test
    public void optimizerEnabledWithStandaloneMetadataLogsWarningAndContinues()
            throws Exception {
        Path logDir = tmpFolder.newFolder("log").toPath();
        Path dataDir = tmpFolder.newFolder("data").toPath();
        Path metadataDir = tmpFolder.newFolder("metadata").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "file");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED, "true");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS, "10000");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "100");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        // FileMetadataStorageManager is the standalone-mode metadata backend.
        // The engine cannot wire the segment registry against it, so it must
        // log a WARNING and continue without a registry — existing standalone
        // deployments that accidentally set indexing.optimizer.enabled MUST
        // NOT regress to a hard startup failure.
        FileMetadataStorageManager fsMeta = new FileMetadataStorageManager(metadataDir);
        fsMeta.start();
        fsMeta.ensureDefaultTableSpace("local", "local", 0, 1);

        // Capture the engine's WARNING-level emissions so the assertion is
        // observable rather than relying on a side-effect of stderr.
        WarningCaptor captor = new WarningCaptor();
        Logger engineLogger = Logger.getLogger(IndexingServiceEngine.class.getName());
        engineLogger.addHandler(captor);
        Level previousLevel = engineLogger.getLevel();
        engineLogger.setLevel(Level.ALL);

        try {
            FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
            MemoryManager mm = new MemoryManager(
                    128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

            try (IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config)) {
                engine.setMetadataStorageManager(fsMeta);
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);
                engine.start();

                assertNull("non-ZK metadata manager must NOT yield a registry handle, "
                        + "even when indexing.optimizer.enabled=true",
                        engine.getSegmentRegistry());
                assertTrue("a WARNING about optimizer.enabled+non-ZK metadata must"
                        + " surface in the engine logs, captured: " + captor.snapshot(),
                        captor.sawWarningContaining(
                                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED));
            }
        } finally {
            engineLogger.removeHandler(captor);
            engineLogger.setLevel(previousLevel);
            fsMeta.close();
        }
    }

    /**
     * Regression test for issue #514: DROP_INDEX must close and remove the
     * {@link SegmentAssignmentWatcher} that was armed when the index was
     * created. Without the fix, the watcher's background refresh executor
     * kept running indefinitely after the index was dropped, and its ZK
     * callbacks could fire against a store that had already been closed.
     */
    @Test
    public void dropIndexClosesSegmentWatcher() throws Exception {
        Path logDir = tmpFolder.newFolder("log-drop").toPath();
        Path dataDir = tmpFolder.newFolder("data-drop").toPath();
        String basePath = "/herd-test-514-drop-watcher";

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "file");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED, "true");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS, "10000");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "100");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        try (ZookeeperMetadataStorageManager zkMeta = new ZookeeperMetadataStorageManager(
                zkServer.getConnectString(), 30000, basePath)) {
            zkMeta.start(true);
            zkMeta.ensureDefaultTableSpace("local", "local", 0, 1);

            FileDataStorageManager dsm = new FileDataStorageManager(dataDir);
            MemoryManager mm = new MemoryManager(
                    128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

            try (IndexingServiceEngine engine =
                    new IndexingServiceEngine(logDir, dataDir, config)) {
                engine.setMetadataStorageManager(zkMeta);
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);
                engine.start();

                // Apply CREATE_TABLE + CREATE_INDEX so the engine creates a
                // PersistentVectorStore and arms a SegmentAssignmentWatcher for it.
                Table table = buildDropTable("drop_t");
                Index idx = buildDropVectorIndex("drop_v", "drop_t");
                engine.applyEntry(new LogSequenceNumber(1, 1),
                        LogEntryFactory.createTable(table, null));
                engine.applyEntry(new LogSequenceNumber(1, 2),
                        LogEntryFactory.createIndex(idx, null));

                // Verify the watcher was registered.
                Map<String, SegmentAssignmentWatcher> watchersBefore =
                        engine.snapshotSegmentWatchersForTest();
                assertEquals("watcher must be registered after CREATE_INDEX",
                        1, watchersBefore.size());

                // Apply DROP_INDEX: must close and remove the watcher.
                engine.applyEntry(new LogSequenceNumber(1, 3),
                        LogEntryFactory.dropIndex("drop_v", null));
                engine.awaitPendingWorkForTest();

                Map<String, SegmentAssignmentWatcher> watchersAfter =
                        engine.snapshotSegmentWatchersForTest();
                assertEquals(
                        "DROP_INDEX must remove the SegmentAssignmentWatcher "
                                + "(issue #514 watcher-leak fix)",
                        0, watchersAfter.size());
            }
        }
    }

    private static Table buildDropTable(String name) {
        int h = name.hashCode() & 0x7FFFFFFF;
        return Table.builder()
                .name(name)
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .tableId(h == 0 ? 1 : h)
                .build();
    }

    private static Index buildDropVectorIndex(String name, String tableName) {
        return Index.builder()
                .name(name)
                .table(tableName)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Captures WARNING-level log records emitted by the engine logger so that
     * assertions can verify a WARNING was actually produced. We keep the
     * implementation minimal — no formatting, no level filter beyond what
     * java.util.logging delivers — because the test only cares about message
     * substring matches.
     */
    private static final class WarningCaptor extends Handler {
        private final java.util.List<String> warnings =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void publish(LogRecord record) {
            if (closed.get() || record == null) {
                return;
            }
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                // record.getMessage() is the FORMAT (e.g. "{0}=true but ..."); we
                // resolve the parameters with String.valueOf to produce the
                // human-readable text the operator would see in the logs.
                String msg = record.getMessage();
                Object[] params = record.getParameters();
                if (msg != null && params != null) {
                    for (int i = 0; i < params.length; i++) {
                        msg = msg.replace("{" + i + "}", String.valueOf(params[i]));
                    }
                }
                warnings.add(msg);
            }
        }

        boolean sawWarningContaining(String needle) {
            synchronized (warnings) {
                for (String w : warnings) {
                    if (w != null && w.contains(needle)) {
                        return true;
                    }
                }
                return false;
            }
        }

        String snapshot() {
            synchronized (warnings) {
                return new java.util.ArrayList<>(warnings).toString();
            }
        }

        @Override
        public void flush() {
            // no-op: records are kept in memory
        }

        @Override
        public void close() {
            closed.set(true);
            warnings.clear();
        }
    }
}
