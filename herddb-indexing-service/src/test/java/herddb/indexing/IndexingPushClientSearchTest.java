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
import com.google.protobuf.ByteString;
import herddb.codec.RecordSerializer;
import herddb.indexing.proto.PushEntriesResponse;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Coverage for {@link IndexingPushClient#search}: pushes a tiny dataset into
 * an embedded push-mode indexing service, then issues {@code Search} RPCs
 * via the new client wrapper and asserts that the returned primary keys
 * round-trip through HerdDB's record serialization.
 *
 * <p>This is the {@code VectorBench} {@code --protocol grpc} query/recall
 * phase's only contract with the indexing service — keeping a focused test
 * here means failures are diagnosed at the gRPC boundary, not at the
 * end-to-end bench level.
 */
public class IndexingPushClientSearchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private EmbeddedIndexingService service;
    private IndexingPushClient pushClient;

    @After
    public void tearDown() throws Exception {
        if (pushClient != null) {
            pushClient.close();
        }
        if (service != null) {
            service.close();
        }
    }

    private static Table vectorTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("id", ColumnTypes.LONG)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("id")
                .build();
    }

    private static Index vectorIndex() {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private PushEntriesResponse push(long ledger, long firstOffset, List<LogEntry> entries) {
        List<LogSequenceNumber> lsns = new ArrayList<>();
        List<ByteBuf> bufs = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            lsns.add(new LogSequenceNumber(ledger, firstOffset + i));
            bufs.add(entries.get(i).serializeAsByteBuf());
        }
        try {
            return pushClient.pushEntries(lsns, bufs);
        } finally {
            for (ByteBuf b : bufs) {
                b.release();
            }
        }
    }

    private void awaitVectorCount(long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = pushClient.getIndexStatus("default", "vectable", "vidx").getVectorCount();
            if (last >= expected) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("indexed vector count did not reach " + expected + " (last=" + last + ")");
    }

    private static EmbeddedIndexingService startPushService(Path logDir, Path dataDir) throws Exception {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, new IndexingServerConfiguration(props));
        svc.start();
        return svc;
    }

    @Test
    public void searchReturnsPrimaryKeysForPushedVectors() throws Exception {
        service = startPushService(folder.newFolder("log").toPath(), folder.newFolder("data").toPath());
        pushClient = new IndexingPushClient(service.getAddress());

        Table table = vectorTable();
        push(1, 1, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(vectorIndex(), null)));

        // Push 30 vectors with deterministic IDs 0..29 in a single batch.
        List<LogEntry> inserts = new ArrayList<>();
        int n = 30;
        int dim = 8;
        for (long id = 0; id < n; id++) {
            float[] v = new float[dim];
            // A simple ramp keyed by id so neighbours are predictable.
            for (int j = 0; j < dim; j++) {
                v[j] = id + j * 0.5f;
            }
            Record r = RecordSerializer.makeRecord(table, "id", id, "vec", v);
            inserts.add(LogEntryFactory.insert(table, r.key, r.value, null));
        }
        push(1, 3, inserts);

        awaitVectorCount(n, 40_000);
        service.getEngine().awaitPendingWorkForTest();

        // Query with the exact vector for id=10 — the nearest neighbour must be id=10.
        float[] query = new float[dim];
        long expectedId = 10;
        for (int j = 0; j < dim; j++) {
            query[j] = expectedId + j * 0.5f;
        }
        SearchResponse response = pushClient.search("default", "vectable", "vidx", query, 5);
        assertNotNull(response);
        assertTrue("expected at least one result, got " + response.getResultsCount(),
                response.getResultsCount() > 0);
        assertFalse("returnScore is false by default — server must not populate score",
                anyResultHasNonZeroScore(response));

        // Deserialize the top result and verify it is id=10.
        SearchResult top = response.getResults(0);
        Bytes pk = Bytes.from_array(toByteArray(top.getPrimaryKey()));
        Object value = RecordSerializer.deserializePrimaryKey(pk, table);
        assertEquals("top hit must be the exact-match id", Long.valueOf(expectedId), value);

        // Verify the returned IDs are a subset of pushed IDs (sanity check on round-trip).
        Set<Long> seen = new HashSet<>();
        for (SearchResult r : response.getResultsList()) {
            Bytes b = Bytes.from_array(toByteArray(r.getPrimaryKey()));
            Long id = (Long) RecordSerializer.deserializePrimaryKey(b, table);
            seen.add(id);
            assertTrue("returned id must be one of the pushed ids: " + id,
                    id >= 0 && id < n);
        }
        assertEquals("results must not contain duplicates",
                response.getResultsCount(), seen.size());
    }

    private static byte[] toByteArray(ByteString bs) {
        byte[] out = new byte[bs.size()];
        bs.copyTo(out, 0);
        return out;
    }

    private static boolean anyResultHasNonZeroScore(SearchResponse response) {
        for (SearchResult r : response.getResultsList()) {
            if (r.getScore() != 0.0f) {
                return true;
            }
        }
        return false;
    }
}
