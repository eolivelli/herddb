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
import static org.junit.Assert.assertNull;
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
 * Issue #509: verifies the two drop-cleanup methods on {@link VectorIndexManager}:
 *
 * <ul>
 *   <li>{@link VectorIndexManager#notifyIsOfDrop()} — eagerly notifies the remote
 *       IndexingService to begin cleanup. This is what SQL DROP paths call
 *       (via {@code disposeIndexManager()}), dispatched asynchronously so the
 *       DDL write lock is not held during gRPC fan-out.</li>
 *   <li>{@link VectorIndexManager#dropIndexData()} — cleans up only local HerdDB
 *       catalog storage via {@code dataStorageManager.dropIndex()}. Called from
 *       follower-download and restore paths (which must NOT fire IS cleanup because
 *       the index remains live in the cluster schema).</li>
 * </ul>
 *
 * <p>Tests:
 * <ol>
 *   <li>Normal IS path: {@code notifyIsOfDrop()} calls
 *       {@code RemoteVectorIndexService.dropIndex()} with the correct
 *       (tablespace, table, indexName) arguments.</li>
 *   <li>Null-IS path: supplier returns {@code null} — {@code notifyIsOfDrop()}
 *       completes without NPE.</li>
 *   <li>Failing-IS path: IS {@code dropIndex()} throws — the exception is
 *       swallowed (logged as WARNING) and {@code notifyIsOfDrop()} returns
 *       normally.</li>
 *   <li>Local-only path: {@code dropIndexData()} does NOT notify the IS; it only
 *       calls {@code dataStorageManager.dropIndex()} for catalog cleanup.</li>
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
     * recent {@code dropIndex} call. All other methods return harmless defaults.
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
     * Stub that throws on {@code dropIndex()} to exercise the "IS is down" path
     * where the exception must be swallowed by {@code notifyIsOfDrop()}.
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
     * Normal IS path: {@code notifyIsOfDrop()} calls
     * {@code RemoteVectorIndexService.dropIndex()} with the correct
     * (tablespace UUID, table name, index name) triple.
     */
    @Test
    public void notifyIsOfDropCallsRemoteServiceWithCorrectArgs() {
        RecordingRemoteService mockIs = new RecordingRemoteService();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Index ix = buildVectorIndex();

        VectorIndexManager vim = new VectorIndexManager(
                ix,
                /* tableManager     */ null,
                /* log              */ null,
                dsm,
                TS_UUID,
                /* transaction      */ 0L,
                /* writeLockTimeout */ 30_000,
                /* readLockTimeout  */ 30_000,
                () -> mockIs,
                NullStatsLogger.INSTANCE);

        vim.notifyIsOfDrop();

        assertEquals("IS.dropIndex tablespace", TS_UUID, mockIs.lastDropTablespace);
        assertEquals("IS.dropIndex table", TABLE, mockIs.lastDropTable);
        assertEquals("IS.dropIndex indexName", INDEX_NAME, mockIs.lastDropIndex);
    }

    /**
     * Null-IS path: when the supplier returns {@code null} (no IS configured),
     * {@code notifyIsOfDrop()} must complete without NPE.
     */
    @Test
    public void notifyIsOfDropWithNullServiceDoesNotThrow() {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Index ix = buildVectorIndex();

        VectorIndexManager vim = new VectorIndexManager(
                ix, null, null, dsm, TS_UUID, 0L, 30_000, 30_000,
                // Supplier explicitly returns null — no IS configured.
                () -> null,
                NullStatsLogger.INSTANCE);

        // Must not throw.
        vim.notifyIsOfDrop();
    }

    /**
     * Failing-IS path: when {@code RemoteVectorIndexService.dropIndex()} throws
     * (e.g. pod restart, transient gRPC error), the exception must be swallowed
     * and logged as WARNING — {@code notifyIsOfDrop()} must still return normally.
     */
    @Test
    public void notifyIsOfDropSwallowsIsRuntimeFailure() {
        FailingRemoteService failingIs = new FailingRemoteService();
        Index ix = buildVectorIndex();

        VectorIndexManager vim = new VectorIndexManager(
                ix, null, null, new MemoryDataStorageManager(), TS_UUID, 0L, 30_000, 30_000,
                () -> failingIs,
                NullStatsLogger.INSTANCE);

        // Must not propagate the RuntimeException thrown by the IS.
        vim.notifyIsOfDrop();

        assertTrue("IS.dropIndex must have been called (even though it threw)",
                failingIs.dropCalled);
    }

    /**
     * Local-only path: {@code dropIndexData()} must NOT notify the IS — it only
     * calls {@code dataStorageManager.dropIndex()} for local catalog cleanup.
     * This is important because {@code dropIndexData()} is also called from
     * follower-download and restore paths where the IS state must remain intact.
     */
    @Test
    public void dropIndexDataOnlyCallsLocalDsmDropAndDoesNotNotifyIs() throws Exception {
        RecordingRemoteService mockIs = new RecordingRemoteService();
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
                ix,
                /* tableManager     */ null,
                /* log              */ null,
                recordingDsm,
                TS_UUID,
                /* transaction      */ 0L,
                /* writeLockTimeout */ 30_000,
                /* readLockTimeout  */ 30_000,
                () -> mockIs,
                NullStatsLogger.INSTANCE);

        vim.dropIndexData();

        // (1) Local catalog storage must be cleaned via dataStorageManager.dropIndex.
        assertNotNull("dataStorageManager.dropIndex must have been called", dsmDropCall.get());
        assertEquals("dsm.dropIndex tableSpace must be tableSpaceUUID", TS_UUID, dsmDropCall.get()[0]);
        assertEquals("dsm.dropIndex name must be the index UUID", ix.uuid, dsmDropCall.get()[1]);

        // (2) IS must NOT have been called — dropIndexData() is only for local cleanup.
        assertNull("IS must NOT be notified by dropIndexData() (follower-download/restore safety)",
                mockIs.lastDropIndex);
    }
}
