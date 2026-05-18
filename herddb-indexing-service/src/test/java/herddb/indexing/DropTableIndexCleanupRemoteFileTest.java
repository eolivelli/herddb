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
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.RemoteFileServer;
import herddb.remote.RemoteFileServiceClient;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
 * Mini-cluster DROP cleanup test: real {@link RemoteFileServer} backed by
 * its default local object storage, real {@link RemoteFileDataStorageManager}
 * + multipart writer/deleter path, and a real {@link PersistentVectorStore}
 * so segment files actually land on the file server's disk.
 *
 * <p>The minimal-setup
 * {@link DropTableIndexCleanupTest} verifies the engine fix against a
 * pure in-memory data storage manager. This test extends that coverage
 * to the remote-file path: it confirms that a {@code DROP_INDEX} (or
 * {@code DROP_TABLE}) on the engine actually causes the
 * {@link RemoteFileDataStorageManager} to issue a delete-by-prefix to
 * the file server, and that the on-disk directory under the file
 * server's data root is empty afterwards. Without the issue #383 fix,
 * those segment directories would survive forever.
 */
public class DropTableIndexCleanupRemoteFileTest {

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

    private static final int DIM = 16;

    private static Table buildTable(String name) {
        // Issue #408: deterministic non-zero tableId — see DropTableIndexCleanupTest.
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

    private static Index buildVectorIndex(String name, String tableName) {
        return Index.builder()
                .name(name)
                .table(tableName)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private static float[] vec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Counts every regular file under the file server's data directory
     * that belongs to a logical vector index identified by {@code storeUUID}.
     * That includes both files under {@code /<storeUUID>/...} (index pages,
     * the parent index dir) and files under {@code /<storeUUID>_segN/...}
     * (per-segment multipart files: graph and map). Without counting both
     * we would miss the actual graph + map blobs that
     * {@link PersistentVectorStore} writes per sealed segment.
     */
    private static int countRemoteFilesForUuid(Path serverDataDir, String storeUUID) throws IOException {
        if (!Files.exists(serverDataDir)) {
            return 0;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(serverDataDir)) {
            return (int) walk
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String s = p.toString();
                        return s.contains("/" + storeUUID + "/")
                                || s.contains("/" + storeUUID + "_");
                    })
                    .count();
        }
    }

    /**
     * End-to-end DROP_INDEX cleanup against the remote file server. The
     * setup mirrors {@code IndexingServiceWithS3E2ETest.Topology} but
     * with the local-object-storage backend, keeping the test fast and
     * dependency-light.
     */
    @Test
    public void dropIndexErasesRemoteStorageData() throws Exception {
        Path fileServerDataDir = folder.newFolder("file-server").toPath();
        RemoteFileServer fileServer = new RemoteFileServer(
                "localhost", 0, fileServerDataDir, 4);
        fileServer.start();
        try {
            String fileServerAddr = "localhost:" + fileServer.getPort();
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    Collections.singletonList(fileServerAddr))) {
                Path engineMetadata = folder.newFolder("engine-metadata").toPath();
                Path engineTmp = folder.newFolder("engine-tmp").toPath();
                RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                        engineMetadata, engineTmp, 1024 * 1024, client);
                MemoryManager mm = new MemoryManager(
                        64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

                // We use storage.type=memory so the engine does not
                // overwrite our custom vector-store factory in start();
                // the PersistentVectorStore we wire below still talks to
                // the real RemoteFileDataStorageManager, so the
                // DROP-cleanup contract is exercised against the
                // remote-file path even though the engine's own classifier
                // is "memory".
                Properties props = new Properties();
                props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
                IndexingServerConfiguration cfg = new IndexingServerConfiguration(props);

                Path engineLog = folder.newFolder("engine-log").toPath();
                Path engineData = folder.newFolder("engine-data").toPath();
                IndexingServiceEngine engine = new IndexingServiceEngine(
                        engineLog, engineData, cfg);
                engine.setMetadataStorageManager(newMeta());
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);

                AtomicReference<PersistentVectorStore> spy = new AtomicReference<>();
                engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                               dataDirectory, indexProperties) -> {
                    PersistentVectorStore pvs = new PersistentVectorStore(
                            indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                            dataDirectory, dsm, mm,
                            16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                            Long.MAX_VALUE,
                            VectorSimilarityFunction.EUCLIDEAN);
                    try {
                        pvs.start();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    spy.set(pvs);
                    return pvs;
                });
                engine.setWatermarkStore(new WatermarkStore() {
                    @Override
                    public WatermarkSnapshot load() {
                        return WatermarkSnapshot.START_OF_TIME;
                    }

                    @Override
                    public void save(WatermarkSnapshot snapshot) throws IOException {
                    }
                });

                try {
                    engine.start();

                    Table table = buildTable("t1");
                    Index ix = buildVectorIndex("vidx", "t1");
                    engine.applyEntry(new LogSequenceNumber(1, 1),
                            LogEntryFactory.createTable(table, null));
                    engine.applyEntry(new LogSequenceNumber(1, 2),
                            LogEntryFactory.createIndex(ix, null));

                    String storeUUID = spy.get().getStoreUUID();
                    assertNotNull(storeUUID);

                    Random rng = new Random(7);
                    LogSequenceNumber last = null;
                    for (int i = 0; i < 256; i++) {
                        Record rec = RecordSerializer.makeRecord(table,
                                "pk", "k" + i, "vec", vec(rng));
                        LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                        last = new LogSequenceNumber(1, 100 + i);
                        engine.applySingleEntryForTest(last, insert);
                    }
                    engine.awaitPendingWorkForTest();
                    engine.setLastProcessedLsnForTest(last);
                    engine.forceCheckpointAndSaveWatermark();

                    int beforeDrop = countRemoteFilesForUuid(fileServerDataDir, storeUUID);
                    if (beforeDrop == 0) {
                        // Diagnostic: dump the file server's data dir so we
                        // can see why the segment file lookup missed.
                        StringBuilder sb = new StringBuilder("file server tree:\n");
                        try (java.util.stream.Stream<Path> w = Files.walk(fileServerDataDir)) {
                            w.forEach(p -> sb.append("  ").append(p).append('\n'));
                        }
                        sb.append("expected UUID substring: /").append(storeUUID).append("/");
                        throw new AssertionError(
                                "file server must hold segment files for the index after a checkpoint; "
                                        + "got 0. " + sb);
                    }

                    // ---- DROP_INDEX ----
                    engine.applyEntry(new LogSequenceNumber(1, 999),
                            LogEntryFactory.dropIndex("vidx", null));
                    engine.awaitPendingWorkForTest();
                    engine.awaitPendingDeletionsForTest();

                    int afterDrop = countRemoteFilesForUuid(fileServerDataDir, storeUUID);
                    assertEquals(
                            "DROP_INDEX must remove every file the file server held for the index "
                                    + "(issue #383); UUID=" + storeUUID,
                            0, afterDrop);
                } finally {
                    engine.close();
                }
            }
        } finally {
            fileServer.stop();
        }
    }

    /**
     * End-to-end DROP_TABLE cleanup against the remote file server. Same
     * shape as the DROP_INDEX test but creates two vector indexes on the
     * same table; DROP_TABLE must remove every byte for both.
     */
    @Test
    public void dropTableErasesRemoteStorageDataForAllIndexes() throws Exception {
        Path fileServerDataDir = folder.newFolder("file-server").toPath();
        RemoteFileServer fileServer = new RemoteFileServer(
                "localhost", 0, fileServerDataDir, 4);
        fileServer.start();
        try {
            String fileServerAddr = "localhost:" + fileServer.getPort();
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                    Collections.singletonList(fileServerAddr))) {
                Path engineMetadata = folder.newFolder("engine-metadata").toPath();
                Path engineTmp = folder.newFolder("engine-tmp").toPath();
                RemoteFileDataStorageManager dsm = new RemoteFileDataStorageManager(
                        engineMetadata, engineTmp, 1024 * 1024, client);
                MemoryManager mm = new MemoryManager(
                        64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

                // We use storage.type=memory so the engine does not
                // overwrite our custom vector-store factory in start();
                // the PersistentVectorStore we wire below still talks to
                // the real RemoteFileDataStorageManager, so the
                // DROP-cleanup contract is exercised against the
                // remote-file path even though the engine's own classifier
                // is "memory".
                Properties props = new Properties();
                props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
                IndexingServerConfiguration cfg = new IndexingServerConfiguration(props);

                Path engineLog = folder.newFolder("engine-log").toPath();
                Path engineData = folder.newFolder("engine-data").toPath();
                IndexingServiceEngine engine = new IndexingServiceEngine(
                        engineLog, engineData, cfg);
                engine.setMetadataStorageManager(newMeta());
                engine.setDataStorageManager(dsm);
                engine.setMemoryManager(mm);

                List<PersistentVectorStore> stores = new ArrayList<>();
                engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                               dataDirectory, indexProperties) -> {
                    PersistentVectorStore pvs = new PersistentVectorStore(
                            indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                            dataDirectory, dsm, mm,
                            16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                            Long.MAX_VALUE,
                            VectorSimilarityFunction.EUCLIDEAN);
                    try {
                        pvs.start();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    synchronized (stores) {
                        stores.add(pvs);
                    }
                    return pvs;
                });
                engine.setWatermarkStore(new WatermarkStore() {
                    @Override
                    public WatermarkSnapshot load() {
                        return WatermarkSnapshot.START_OF_TIME;
                    }

                    @Override
                    public void save(WatermarkSnapshot snapshot) throws IOException {
                    }
                });

                try {
                    engine.start();

                    Table table = buildTable("t2");
                    Index ix1 = buildVectorIndex("vidx_a", "t2");
                    Index ix2 = buildVectorIndex("vidx_b", "t2");
                    engine.applyEntry(new LogSequenceNumber(1, 1),
                            LogEntryFactory.createTable(table, null));
                    engine.applyEntry(new LogSequenceNumber(1, 2),
                            LogEntryFactory.createIndex(ix1, null));
                    engine.applyEntry(new LogSequenceNumber(1, 3),
                            LogEntryFactory.createIndex(ix2, null));

                    List<String> uuids = new ArrayList<>();
                    synchronized (stores) {
                        for (PersistentVectorStore s : stores) {
                            uuids.add(s.getStoreUUID());
                        }
                    }
                    assertEquals(2, uuids.size());

                    Random rng = new Random(11);
                    LogSequenceNumber last = null;
                    for (int i = 0; i < 256; i++) {
                        Record rec = RecordSerializer.makeRecord(table,
                                "pk", "k" + i, "vec", vec(rng));
                        LogEntry insert = LogEntryFactory.insert(table, rec.key, rec.value, null);
                        last = new LogSequenceNumber(1, 100 + i);
                        engine.applySingleEntryForTest(last, insert);
                    }
                    engine.awaitPendingWorkForTest();
                    engine.setLastProcessedLsnForTest(last);
                    engine.forceCheckpointAndSaveWatermark();

                    for (String uuid : uuids) {
                        int beforeDrop = countRemoteFilesForUuid(fileServerDataDir, uuid);
                        assertTrue(
                                "file server must hold segment files for index UUID " + uuid
                                        + " after a checkpoint; got " + beforeDrop,
                                beforeDrop > 0);
                    }

                    // ---- DROP_TABLE ----
                    engine.applyEntry(new LogSequenceNumber(1, 999),
                            LogEntryFactory.dropTable(table, null));
                    engine.awaitPendingWorkForTest();
                    engine.awaitPendingDeletionsForTest();

                    for (String uuid : uuids) {
                        int afterDrop = countRemoteFilesForUuid(fileServerDataDir, uuid);
                        assertEquals(
                                "DROP_TABLE must remove every file the file server held for index UUID " + uuid
                                        + " (issue #383)",
                                0, afterDrop);
                    }
                } finally {
                    engine.close();
                }
            }
        } finally {
            fileServer.stop();
        }
    }
}
