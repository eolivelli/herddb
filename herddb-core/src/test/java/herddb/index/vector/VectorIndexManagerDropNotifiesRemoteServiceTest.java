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
package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Issue #509: verifies that {@link VectorIndexManager#dropIndexData()} eagerly
 * notifies the remote IndexingService to begin cleanup, in addition to
 * cleaning local HerdDB catalog storage.
 *
 * <p>Tests cover:
 * <ol>
 *   <li>Normal path: IS is available, {@code dropIndex()} is called with the
 *       correct (tablespace, table, indexName) arguments and local catalog
 *       storage is also cleaned via {@code dataStorageManager.dropIndex()}.</li>
 *   <li>Null-IS path: supplier returns {@code null} (no IS configured) —
 *       {@code dropIndexData()} completes normally without NPE.</li>
 *   <li>Failing-IS path: IS {@code dropIndex()} throws — the exception is
 *       swallowed (logged as WARNING) and local catalog storage is still
 *       cleaned.</li>
 * </ol>
 */
public class VectorIndexManagerDropNotifiesRemoteServiceTest {

    private static final String TS_UUID = "test-ts-uuid";
    private static final String TABLE = "t1";
    private static final String INDEX_NAME = "vidx";

    private static Table buildTable() {
        return Table.builder()
                .name(TABLE)
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex() {
        return Index.builder()
                .name(INDEX_NAME)
                .table(TABLE)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    /**
     * Minimal stub of {@link RemoteVectorIndexService} that records the most
     * recent {@code dropIndex} call and does nothing else. All other methods
     * return harmless defaults so the test never fails on an unexpected path.
     */
    private static final class RecordingRemoteService implements RemoteVectorIndexService {

        volatile String lastDropTablespace;
        volatile String lastDropTable;
        volatile String lastDropIndex;

        @Override
        public void dropIndex(String tablespace, String table, String indexName) {
            this.lastDropTablespace = tablespace;
            this.lastDropTable = table;
            this.lastDropIndex = indexName;
        }

        @Override
        public List<Map.Entry<Bytes, Float>> search(String tablespace, String table,
                                                     String index, float[] vector, int topK) {
            return Collections.emptyList();
        }

        @Override
        public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
            return new IndexStatusInfo(0, 0, 0, 0, 0, 0, 0, 0, "OK", 0, 0);
        }

        @Override
        public boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber,
                                      long timeoutMs) {
            return true;
        }

        @Override
        public Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
            return Optional.empty();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Stub that throws on {@code dropIndex()} to exercise the "IS is down"
     * path where the exception must be swallowed.
     */
    private static final class FailingRemoteService implements RemoteVectorIndexService {

        volatile boolean dropCalled = false;

        @Override
        public void dropIndex(String tablespace, String table, String indexName) {
            dropCalled = true;
            throw new RuntimeException("simulated IS failure (pod restart)");
        }

        @Override
        public List<Map.Entry<Bytes, Float>> search(String tablespace, String table,
                                                     String index, float[] vector, int topK) {
            return Collections.emptyList();
        }

        @Override
        public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
            return new IndexStatusInfo(0, 0, 0, 0, 0, 0, 0, 0, "OK", 0, 0);
        }

        @Override
        public boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber,
                                      long timeoutMs) {
            return true;
        }

        @Override
        public Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
            return Optional.empty();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Normal path: IS is reachable; {@code dropIndexData()} calls
     * {@code RemoteVectorIndexService.dropIndex()} with the correct arguments
     * AND calls {@code dataStorageManager.dropIndex()} for local cleanup.
     */
    @Test
    public void dropIndexDataNotifiesRemoteServiceWithCorrectArgs() throws Exception {
        RecordingRemoteService mockIs = new RecordingRemoteService();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Index ix = buildVectorIndex();

        // Record calls to dsm.dropIndex so we can verify the catalog UUID.
        AtomicReference<String[]> dsmDropCall = new AtomicReference<>();
        MemoryDataStorageManager recordingDsm = new MemoryDataStorageManager() {
            @Override
            public void dropIndex(String tableSpace, String name)
                    throws herddb.storage.DataStorageManagerException {
                dsmDropCall.set(new String[]{tableSpace, name});
                super.dropIndex(tableSpace, name);
            }
        };

        VectorIndexManager vim = new VectorIndexManager(
                ix,
                /* tableManager   */ null,
                /* log            */ null,
                recordingDsm,
                TS_UUID,
                /* transaction    */ 0L,
                /* writeLockTimeout */ 30_000,
                /* readLockTimeout  */ 30_000,
                () -> mockIs,
                NullStatsLogger.INSTANCE);

        vim.dropIndexData();

        // (1) IS was notified with the correct (tablespace, table, index) triple.
        assertEquals("IS.dropIndex tablespace", TS_UUID, mockIs.lastDropTablespace);
        assertEquals("IS.dropIndex table", TABLE, mockIs.lastDropTable);
        assertEquals("IS.dropIndex indexName", INDEX_NAME, mockIs.lastDropIndex);

        // (2) Local catalog storage was also cleaned via dataStorageManager.dropIndex.
        assertNotNull("dataStorageManager.dropIndex must have been called", dsmDropCall.get());
        assertEquals("dsm.dropIndex tableSpace", TS_UUID, dsmDropCall.get()[0]);
        // The uuid argument is the HerdDB catalog UUID of the index object.
        assertEquals("dsm.dropIndex index UUID", ix.uuid, dsmDropCall.get()[1]);
    }

    /**
     * Null-IS path: when the supplier returns {@code null} (no IS configured),
     * {@code dropIndexData()} must complete without NPE and still clean up
     * local catalog storage.
     */
    @Test
    public void dropIndexDataWithNullSupplierDoesNotThrow() throws Exception {
        AtomicReference<String[]> dsmDropCall = new AtomicReference<>();
        MemoryDataStorageManager recordingDsm = new MemoryDataStorageManager() {
            @Override
            public void dropIndex(String tableSpace, String name)
                    throws herddb.storage.DataStorageManagerException {
                dsmDropCall.set(new String[]{tableSpace, name});
                super.dropIndex(tableSpace, name);
            }
        };
        Index ix = buildVectorIndex();

        VectorIndexManager vim = new VectorIndexManager(
                ix, null, null, recordingDsm, TS_UUID, 0L, 30_000, 30_000,
                // Supplier explicitly returns null — no IS configured.
                () -> null,
                NullStatsLogger.INSTANCE);

        // Must not throw.
        vim.dropIndexData();

        // Local catalog cleanup still runs.
        assertNotNull("dsm.dropIndex must be called even without an IS",
                dsmDropCall.get());
        assertEquals(TS_UUID, dsmDropCall.get()[0]);
    }

    /**
     * Failing-IS path: when {@code RemoteVectorIndexService.dropIndex()} throws
     * (e.g. pod restart, transient gRPC error), the exception must be swallowed
     * and logged as WARNING — {@code dropIndexData()} must still complete and
     * still clean up local catalog storage.
     */
    @Test
    public void dropIndexDataSwallowsIsFailureAndStillCleansLocalStorage() throws Exception {
        FailingRemoteService failingIs = new FailingRemoteService();
        AtomicReference<String[]> dsmDropCall = new AtomicReference<>();
        MemoryDataStorageManager recordingDsm = new MemoryDataStorageManager() {
            @Override
            public void dropIndex(String tableSpace, String name)
                    throws herddb.storage.DataStorageManagerException {
                dsmDropCall.set(new String[]{tableSpace, name});
                super.dropIndex(tableSpace, name);
            }
        };
        Index ix = buildVectorIndex();

        VectorIndexManager vim = new VectorIndexManager(
                ix, null, null, recordingDsm, TS_UUID, 0L, 30_000, 30_000,
                () -> failingIs,
                NullStatsLogger.INSTANCE);

        // Must not propagate the exception thrown by the IS.
        vim.dropIndexData();

        assertTrue("IS.dropIndex must have been called (even though it threw)",
                failingIs.dropCalled);
        assertNotNull("dsm.dropIndex must still run after IS failure",
                dsmDropCall.get());
        assertEquals(TS_UUID, dsmDropCall.get()[0]);
    }
}
