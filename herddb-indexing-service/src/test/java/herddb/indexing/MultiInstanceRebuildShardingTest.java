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
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies that {@code rebuild=true} CREATE_INDEX entries
 * propagate the same shard predicate as the live tailer's INSERT path.
 * With N IndexingService instances, each instance must back-fill ONLY
 * the records whose primary key the local
 * {@code IndexingServiceEngine.isAcceptedLocally} accepts; the
 * <strong>union</strong> of the three local stores must cover every
 * pre-staged record exactly once, with no key appearing in two
 * instances.
 *
 * <p>This test uses three independent {@link FakeFullScanDsm} instances
 * (one per engine), each holding the SAME pre-staged record list — a
 * proxy for the production scenario where N IS instances point at the
 * SAME shared remote storage. Step 5 lifts this to a real
 * {@code RemoteFileServer} + 3 IS engines; step 4 covers the sharding
 * logic in isolation so a regression in
 * {@code IndexingServiceEngine.isAcceptedLocally} is caught at the
 * fastest possible test layer.
 *
 * @author enrico.olivelli
 */
public class MultiInstanceRebuildShardingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private static final LogSequenceNumber REBUILD_LSN = new LogSequenceNumber(7L, 13L);

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createIndex(int numShards) {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .property(VectorIndexManager.PROP_REBUILD, "true")
                .property(VectorIndexManager.PROP_REBUILD_LSN,
                        VectorIndexManager.encodeRebuildLsn(REBUILD_LSN))
                .build();
    }

    private IndexingServiceEngine buildEngine(int instanceId, int numInstances,
                                              MemoryDataStorageManager dsm) throws Exception {
        Path logDir = folder.newFolder("log-" + instanceId).toPath();
        Path dataDir = folder.newFolder("data-" + instanceId).toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                String.valueOf(numInstances));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        MemoryMetadataStorageManager memMeta = new MemoryMetadataStorageManager();
        memMeta.start();
        memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(memMeta);
        engine.setDataStorageManager(dsm);
        engine.start();
        return engine;
    }

    private void runShardingScenario(int numInstances, int numShards, int numRecords)
            throws Exception {
        Table table = createTable();
        Index index = createIndex(numShards);

        // Pre-stage the record list — same content for every engine,
        // because in production N IS instances all read from the same
        // shared remote storage.
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add(RecordSerializer.makeRecord(table,
                    "pk", "key" + i,
                    "vec", new float[]{i * 0.1f, i * 0.2f, i * 0.3f}));
        }

        List<IndexingServiceEngine> engines = new ArrayList<>();
        try {
            for (int i = 0; i < numInstances; i++) {
                FakeFullScanDsm dsm = new FakeFullScanDsm(REBUILD_LSN, records);
                IndexingServiceEngine engine = buildEngine(i, numInstances, dsm);
                engines.add(engine);

                // Drive the entries through processEntryForTest to
                // exercise the production tailer code path
                // (classifyForMetrics + lastProcessedLsn advancement
                // + applyEntry switch). Using applyEntry directly
                // would bypass those layers — fine for sharding-only
                // assertions, but parity with the other tests in
                // this suite is preferred.
                engine.processEntryForTest(new LogSequenceNumber(1, 1),
                        LogEntryFactory.createTable(table, null));
                engine.processEntryForTest(new LogSequenceNumber(1, 2),
                        LogEntryFactory.createIndex(index, null));
            }

            // Each engine's local store must contain ONLY records the
            // local shard predicate accepted, and the union of all
            // engines must cover every staged record exactly once.
            //
            // The per-instance owned count is bounded by the
            // deterministic XXHash64 distribution of the keys
            // "key0".."key{numRecords-1}" mod numShards, then
            // rounded by shardId % numInstances. For the chosen
            // (numInstances, numShards) pairs (2,4), (3,6), (5,10)
            // each instance empirically owns numRecords/numInstances
            // ± ~30%; if a future hash-function change skews this
            // distribution, the > 0 / < numRecords assertions below
            // still pin the correctness contract (each instance owns
            // a non-empty proper subset).
            Set<Bytes> unionOfLocalKeys = new HashSet<>();
            int totalLocalRecords = 0;
            float[] queryVector = new float[]{0f, 0f, 0f};
            for (int i = 0; i < numInstances; i++) {
                IndexingServiceEngine engine = engines.get(i);
                // Cache the search result — the InMemoryVectorStore is
                // deterministic, but a future approximate top-K store
                // could return a different set on the second call.
                List<Map.Entry<Bytes, Float>> searchHits = engine.search(
                        "default", "vectable", "vidx", queryVector, numRecords);
                totalLocalRecords += searchHits.size();
                long indexed = engine.getRebuildMetricsForTest().recordsIndexed.sum();
                assertTrue("instance " + i + " must own SOME records (indexed="
                                + indexed + ", numRecords=" + numRecords + ")",
                        indexed > 0);
                assertTrue("instance " + i + " must own a SHARD subset only "
                                + "(indexed=" + indexed + ", numRecords=" + numRecords + ")",
                        indexed < numRecords);
                for (Map.Entry<Bytes, Float> hit : searchHits) {
                    assertFalse("key " + hit.getKey() + " appears in two instances "
                                    + "(numInstances=" + numInstances + ")",
                            unionOfLocalKeys.contains(hit.getKey()));
                    unionOfLocalKeys.add(hit.getKey());
                }
            }
            assertEquals("union of local keys must cover every staged record",
                    numRecords, unionOfLocalKeys.size());
            assertEquals("sum of local record counts must equal staged record count",
                    numRecords, totalLocalRecords);
        } finally {
            for (IndexingServiceEngine engine : engines) {
                try {
                    engine.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Test
    public void threeInstances_sixShards_sixtyRecords_eachOwnsItsShard() throws Exception {
        runShardingScenario(3, 6, 60);
    }

    @Test
    public void twoInstances_fourShards_eightyRecords_eachOwnsItsShard() throws Exception {
        runShardingScenario(2, 4, 80);
    }

    @Test
    public void fiveInstances_tenShards_oneHundredRecords_eachOwnsItsShard() throws Exception {
        runShardingScenario(5, 10, 100);
    }

    private static final class FakeFullScanDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;

        FakeFullScanDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = Objects.requireNonNull(expectedLsn);
            this.records = new ArrayList<>(records);
        }

        @Override
        public void fullTableScan(String tableSpace, String uuid,
                                  LogSequenceNumber sequenceNumber,
                                  FullTableScanConsumer consumer)
                throws DataStorageManagerException {
            TableStatus status = new TableStatus("vectable", expectedLsn,
                    Bytes.longToByteArray(0L), 1L, new HashMap<>());
            consumer.acceptTableStatus(status);
            consumer.acceptPage(0L, Arrays.asList(records.toArray(new Record[0])));
            consumer.endTable();
        }
    }
}
