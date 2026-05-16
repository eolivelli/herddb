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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.indexing.proto.PushEntriesRequest;
import herddb.indexing.proto.PushEntriesResponse;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * End-to-end coverage of the push-based indexing gRPC API: pushing serialized
 * {@code LogEntry} objects into an indexing service running with
 * {@code indexing.log.type=push} builds the vector index without a HerdDB
 * server or a commit log; malformed batches are rejected atomically; and the
 * API distinguishes "not a push service" from "engine still starting".
 */
public class IndexingServicePushGrpcTest {

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

    private EmbeddedIndexingService startService(String logType) throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        EmbeddedIndexingService svc = new EmbeddedIndexingService(
                logDir, dataDir, pushConfig(logType));
        svc.start();
        return svc;
    }

    private static IndexingServerConfiguration pushConfig(String logType) {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE, logType);
        return new IndexingServerConfiguration(props);
    }

    private static Table vectorTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
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

    /**
     * Serializes the entries to pooled direct ByteBufs, pushes them as one
     * batch starting at {@code (ledger, firstOffset)}, and releases the
     * buffers afterwards — mirroring how {@code VectorBench} drives the API.
     */
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

    private long indexedVectorCount() {
        return pushClient.getIndexStatus("default", "vectable", "vidx").getVectorCount();
    }

    private void awaitVectorCount(long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = indexedVectorCount();
            if (last >= expected) {
                return;
            }
            Thread.sleep(50);
        }
        fail("indexed vector count did not reach " + expected + " (last=" + last + ")");
    }

    @Test
    public void pushedEntriesBuildTheVectorIndex() throws Exception {
        service = startService(IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        assertNotNull("engine must run a push tailer", service.getEngine().getPushTailer());
        pushClient = new IndexingPushClient(service.getAddress());

        Table table = vectorTable();
        // DDL batch: CREATE TABLE + CREATE VECTOR INDEX.
        push(1, 1, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(vectorIndex(), null)));

        // 20 non-transactional INSERTs.
        List<LogEntry> inserts = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Record r = RecordSerializer.makeRecord(table,
                    "pk", "key" + i, "vec", new float[]{i, i + 1f, i + 2f});
            inserts.add(LogEntryFactory.insert(table, r.key, r.value, null));
        }
        PushEntriesResponse resp = push(1, 3, inserts);
        assertEquals("server must acknowledge every pushed entry", 20L, resp.getAcceptedCount());

        awaitVectorCount(20, 40_000);
        service.getEngine().awaitPendingWorkForTest();
        List<?> hits = service.getEngine().search("default", "vectable", "vidx",
                new float[]{0f, 1f, 2f}, 100);
        assertEquals("every pushed vector must be searchable", 20, hits.size());
    }

    @Test
    public void pushedEntriesWithinATransactionAreAppliedAtCommit() throws Exception {
        service = startService(IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        pushClient = new IndexingPushClient(service.getAddress());

        Table table = vectorTable();
        push(1, 1, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(vectorIndex(), null)));

        // BEGIN + 5 INSERTs (carrying the transaction id) + COMMIT.
        long txId = 7L;
        List<LogEntry> txBatch = new ArrayList<>();
        txBatch.add(LogEntryFactory.beginTransaction(txId));
        for (int i = 0; i < 5; i++) {
            Record r = RecordSerializer.makeRecord(table,
                    "pk", "tx" + i, "vec", new float[]{i, i, i});
            txBatch.add(new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT,
                    txId, table.tableId, r.key, r.value));
        }
        txBatch.add(LogEntryFactory.commitTransaction(txId));
        PushEntriesResponse resp = push(1, 3, txBatch);
        assertEquals(7L, resp.getAcceptedCount());

        awaitVectorCount(5, 40_000);
        service.getEngine().awaitPendingWorkForTest();
        List<?> hits = service.getEngine().search("default", "vectable", "vidx",
                new float[]{0f, 0f, 0f}, 100);
        assertEquals("every committed transactional vector must be searchable", 5, hits.size());
    }

    @Test
    public void pushBatchWithAMalformedEntryIsRejectedAtomically() throws Exception {
        service = startService(IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        pushClient = new IndexingPushClient(service.getAddress());

        Table table = vectorTable();
        push(1, 1, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(vectorIndex(), null)));

        // Batch: valid INSERT, garbage, valid INSERT. The garbage entry must
        // reject the whole batch — neither valid INSERT may be applied.
        Record r0 = RecordSerializer.makeRecord(table, "pk", "ok0", "vec", new float[]{1, 1, 1});
        Record r2 = RecordSerializer.makeRecord(table, "pk", "ok2", "vec", new float[]{2, 2, 2});
        List<LogSequenceNumber> lsns = Arrays.asList(
                new LogSequenceNumber(1, 3),
                new LogSequenceNumber(1, 4),
                new LogSequenceNumber(1, 5));
        List<ByteBuf> bufs = Arrays.asList(
                LogEntryFactory.insert(table, r0.key, r0.value, null).serializeAsByteBuf(),
                Unpooled.wrappedBuffer(new byte[]{1, 2, 3}),
                LogEntryFactory.insert(table, r2.key, r2.value, null).serializeAsByteBuf());
        try {
            pushClient.pushEntries(lsns, bufs);
            fail("a malformed entry must reject the whole batch");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        } finally {
            for (ByteBuf b : bufs) {
                b.release();
            }
        }
        // All-or-nothing: neither valid INSERT in the rejected batch was applied.
        Thread.sleep(500);
        assertEquals("no entry from a rejected batch may be applied", 0L, indexedVectorCount());
    }

    @Test
    public void pushEntriesIsRejectedWhenNotInPushMode() throws Exception {
        // Default "file" tailer mode — PushEntries must fail fast (permanent).
        service = startService(IndexingServerConfiguration.PROPERTY_LOG_TYPE_DEFAULT);
        assertNull("engine must not run a push tailer in file mode",
                service.getEngine().getPushTailer());
        pushClient = new IndexingPushClient(service.getAddress());
        try {
            push(1, 1, Arrays.asList(LogEntryFactory.noop()));
            fail("PushEntries must be rejected when not in push mode");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
        }
    }

    @Test
    public void pushEntriesDuringEngineStartupIsRetryable() throws Exception {
        // An engine configured for push mode but not yet started has no tailer
        // (the gRPC server binds before engine.start() assigns it). The handler
        // must return a retryable UNAVAILABLE, not the permanent-looking
        // FAILED_PRECONDITION a correct client treats as fatal.
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir,
                pushConfig(IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH));
        assertNull("an unstarted engine has no push tailer yet", engine.getPushTailer());

        IndexingServiceImpl impl = new IndexingServiceImpl(engine, NullStatsLogger.INSTANCE);
        CapturingObserver<PushEntriesResponse> observer = new CapturingObserver<>();
        impl.pushEntries(PushEntriesRequest.newBuilder().build(), observer);

        assertNotNull("a still-starting engine must report an error", observer.error);
        assertEquals(Status.Code.UNAVAILABLE,
                ((StatusRuntimeException) observer.error).getStatus().getCode());
    }

    /** Minimal {@link StreamObserver} that records the terminal error for assertions. */
    private static final class CapturingObserver<T> implements StreamObserver<T> {

        private volatile Throwable error;

        @Override
        public void onNext(T value) {
            // not exercised — this observer is used only for the error path
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            // not exercised — this observer is used only for the error path
        }
    }
}
