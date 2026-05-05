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

package herddb.core;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageCheckpointSnapshot;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #403 (PR review follow-up): verifies that {@code checkPointRunning}
 * is cleared even when a failure originates inside the under-lock Phase C
 * block, so subsequent operations gated on {@code !checkPointRunning} —
 * notably {@code TRUNCATE TABLE} — continue to work after a transient
 * checkpoint failure.
 *
 * <p>Without the fix, a throw inside any of the under-lock Phase C steps
 * ({@code flushMutablePage}, {@code flushNewPageForCheckpoint},
 * {@code keyToPage.prepareCheckpoint}, {@code allocateLivePage}) leaves
 * {@code checkPointRunning=true} forever and permanently breaks
 * {@code TRUNCATE TABLE} on that table.</p>
 */
public class FuzzyCheckpointRunningFlagClearedOnFailureTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void truncateSucceedsAfterPrepareCheckpointFailure() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        FailingPrepareCheckpointStorage storage = new FailingPrepareCheckpointStorage(dataPath);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                storage,
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            // Arm the failure for the next checkpoint. The checkpoint method
            // catches per-table DataStorageManagerException at the
            // TableSpaceManager level (pre-existing behavior), so checkpoint()
            // does not throw — but the inner try/finally in TableManager.Phase C
            // must still clear checkPointRunning.
            storage.armPrepareFailure(true);
            manager.checkpoint();
            storage.armPrepareFailure(false);

            // Without the issue #403 review fix, checkPointRunning stays true
            // and TRUNCATE TABLE throws "TRUNCATE TABLE cannot be executed
            // during a checkpoint". With the fix, TRUNCATE TABLE succeeds.
            execute(manager, "TRUNCATE TABLE tblspace1.t1", Collections.emptyList());

            // Sanity: post-truncate the table is empty.
            try (DataScanner s = scan(manager,
                    "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(0L, s.consume().size());
            }

            // And the checkpoint pipeline is still functional.
            for (int i = 0; i < 5; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("post" + i, 100 + i));
            }
            manager.checkpoint();

            try (DataScanner s = scan(manager,
                    "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(5L, s.consume().size());
            }
        }
    }

    /**
     * {@link FileDataStorageManager} whose {@code createKeyToPageMap}
     * returns a wrapping {@link KeyToPageIndex} that throws on
     * {@code prepareCheckpoint} when armed. Models a transient under-lock
     * Phase C failure.
     */
    private static final class FailingPrepareCheckpointStorage extends FileDataStorageManager {

        private final AtomicBoolean failPrepare = new AtomicBoolean();

        FailingPrepareCheckpointStorage(Path basePath) {
            super(basePath);
        }

        void armPrepareFailure(boolean fail) {
            failPrepare.set(fail);
        }

        @Override
        public KeyToPageIndex createKeyToPageMap(String tableSpace, String uuid,
                herddb.core.MemoryManager memoryManager) throws DataStorageManagerException {
            KeyToPageIndex delegate = super.createKeyToPageMap(tableSpace, uuid, memoryManager);
            return new ThrowOnPrepareKeyToPageIndex(delegate, failPrepare);
        }
    }

    /**
     * Delegating {@link KeyToPageIndex} that throws inside
     * {@code prepareCheckpoint(...)} when the {@link AtomicBoolean} is set.
     */
    private static final class ThrowOnPrepareKeyToPageIndex implements KeyToPageIndex {

        private final KeyToPageIndex delegate;
        private final AtomicBoolean failPrepare;

        ThrowOnPrepareKeyToPageIndex(KeyToPageIndex delegate, AtomicBoolean failPrepare) {
            this.delegate = delegate;
            this.failPrepare = failPrepare;
        }

        @Override
        public KeyToPageCheckpointSnapshot prepareCheckpoint(LogSequenceNumber sequenceNumber, boolean pin)
                throws DataStorageManagerException {
            if (failPrepare.get()) {
                throw new DataStorageManagerException(
                        "injected failure inside prepareCheckpoint (issue #403 review test)");
            }
            return delegate.prepareCheckpoint(sequenceNumber, pin);
        }

        @Override
        public List<PostCheckpointAction> persistCheckpoint(KeyToPageCheckpointSnapshot snapshot)
                throws DataStorageManagerException {
            return delegate.persistCheckpoint(snapshot);
        }

        @Override
        public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
                throws DataStorageManagerException {
            return persistCheckpoint(prepareCheckpoint(sequenceNumber, pin));
        }

        // ---- pure delegation ----
        @Override
        public long getUsedMemory() {
            return delegate.getUsedMemory();
        }

        @Override
        public boolean requireLoadAtStartup() {
            return delegate.requireLoadAtStartup();
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void init() throws DataStorageManagerException {
            delegate.init();
        }

        @Override
        public void start(LogSequenceNumber sequenceNumber, boolean created) throws DataStorageManagerException {
            delegate.start(sequenceNumber, created);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
            delegate.unpinCheckpoint(sequenceNumber);
        }

        @Override
        public void truncate() {
            delegate.truncate();
        }

        @Override
        public void dropData() {
            delegate.dropData();
        }

        @Override
        public java.util.stream.Stream<java.util.Map.Entry<Bytes, Long>> scanner(
                IndexOperation operation,
                StatementEvaluationContext context,
                herddb.model.TableContext tableContext,
                AbstractIndexManager index) throws DataStorageManagerException, herddb.model.StatementExecutionException {
            return delegate.scanner(operation, context, tableContext, index);
        }

        @Override
        public void put(Bytes key, Long currentPage) {
            delegate.put(key, currentPage);
        }

        @Override
        public boolean put(Bytes key, Long newPage, Long expectedPage) {
            return delegate.put(key, newPage, expectedPage);
        }

        @Override
        public boolean containsKey(Bytes key) {
            return delegate.containsKey(key);
        }

        @Override
        public Long get(Bytes key) {
            return delegate.get(key);
        }

        @Override
        public Long remove(Bytes key) {
            return delegate.remove(key);
        }

        @Override
        public boolean isSortedAscending(int[] pkTypes) {
            return delegate.isSortedAscending(pkTypes);
        }

        @Override
        public Bytes getMinKey() {
            return delegate.getMinKey();
        }

        @Override
        public Bytes getMaxKey() {
            return delegate.getMaxKey();
        }
    }
}
