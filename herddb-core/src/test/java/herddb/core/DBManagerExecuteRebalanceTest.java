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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.log.IndexingServiceRebalanceDescriptor;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage for {@code EXECUTE INDEXING_SERVICE_REBALANCE}: parser
 * accepts/rejects the syntax, DBManager updates the tablespace property,
 * writes the LogEntry with the embedded schema snapshot, and is idempotent
 * on re-run with the same N.
 *
 * <p>The LogEntry-write assertion uses the file-mode commit log's recovery
 * path, performed against a freshly-opened DBManager so the recovery is
 * not racing the active writer.
 *
 * @author enrico.olivelli
 */
public class DBManagerExecuteRebalanceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path data, Path logs, Path meta, Path tmo) throws Exception {
        DBManager m = new DBManager("localhost",
                new FileMetadataStorageManager(meta),
                new FileDataStorageManager(data),
                new FileCommitLogManager(logs),
                tmo, null);
        m.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return m;
    }

    private void createTablespace(DBManager m) throws Exception {
        CreateTableSpaceStatement cs = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        m.executeStatement(cs, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        m.waitForTablespace("tblspace1", 10000);
    }

    /**
     * Inspect the persisted commit log via a freshly-opened DBManager. Recovery
     * on an actively-written log is racy by design; closing first is the
     * idiomatic way to read everything back.
     */
    private List<LogEntry> readPersistedLog(Path data, Path logs, Path meta, Path tmo)
            throws Exception {
        List<LogEntry> entries = new ArrayList<>();
        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            m.waitForTablespace("tblspace1", 10000);
            m.getTableSpaceManager("tblspace1").getLog()
                    .recovery(LogSequenceNumber.START_OF_TIME,
                            (lsn, entry) -> entries.add(entry), true);
        }
        return entries;
    }

    private List<LogEntry> filter(List<LogEntry> all, short type) {
        List<LogEntry> out = new ArrayList<>();
        for (LogEntry e : all) {
            if (e.type == type) {
                out.add(e);
            }
        }
        return out;
    }

    @Test
    public void writesLogEntryAndUpdatesTablespaceProperty() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            execute(m, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(m, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1', 4",
                    Collections.emptyList());

            // Tablespace property updated for future CREATE INDEX
            TableSpace ts = m.getMetadataStorageManager().describeTableSpace("tblspace1");
            assertNotNull(ts);
            assertEquals(4, ts.defaultIndexingNumInstances);

            // Existing index is NOT mutated (stamped before the rebalance)
            Index oldIdx = m.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertEquals(String.valueOf(TableSpace.DEFAULT_INDEXING_NUM_INSTANCES_DEFAULT),
                    oldIdx.properties.get(VectorIndexManager.PROP_NUM_INSTANCES));

            // A NEW index picks up the new default
            execute(m, "CREATE VECTOR INDEX vidx2 ON tblspace1.t1(vec)",
                    Collections.emptyList());
            Index newIdx = m.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx2").getIndex();
            assertEquals("4", newIdx.properties.get(VectorIndexManager.PROP_NUM_INSTANCES));
        }

        // Reopen and inspect the log via recovery (safe on a closed log)
        List<LogEntry> rebalances = filter(readPersistedLog(data, logs, meta, tmo),
                LogEntryType.INDEXING_SERVICE_REBALANCE);
        assertEquals(1, rebalances.size());
        IndexingServiceRebalanceDescriptor descriptor =
                IndexingServiceRebalanceDescriptor.deserialize(rebalances.get(0).value.to_array());
        assertEquals(4, descriptor.defaultNumInstances);
        assertTrue("descriptor must contain table t1",
                descriptor.tables.stream().anyMatch(t -> t.name.equals("t1")));
        assertEquals("descriptor must include the existing vector index",
                1, descriptor.vectorIndexes.size());
        assertEquals("vidx", descriptor.vectorIndexes.get(0).name);
    }

    @Test
    public void idempotentOnRepeatedRebalance() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1', 2",
                    Collections.emptyList());
            execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1', 2",
                    Collections.emptyList());
            execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1', 2",
                    Collections.emptyList());

            assertEquals(2, m.getMetadataStorageManager()
                    .describeTableSpace("tblspace1").defaultIndexingNumInstances);
        }
        List<LogEntry> rebalances = filter(readPersistedLog(data, logs, meta, tmo),
                LogEntryType.INDEXING_SERVICE_REBALANCE);
        assertEquals(3, rebalances.size());
    }

    @Test
    public void zeroNumInstancesRejectedByParser() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            assertThrows(StatementExecutionException.class,
                    () -> execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1', 0",
                            Collections.emptyList()));
        }
    }

    @Test
    public void missingNumInstancesRejected() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            assertThrows(StatementExecutionException.class,
                    () -> execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'tblspace1'",
                            Collections.emptyList()));
        }
    }

    @Test
    public void unknownTableSpaceRejected() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            assertThrows(StatementExecutionException.class,
                    () -> execute(m, "EXECUTE INDEXING_SERVICE_REBALANCE 'doesnotexist', 4",
                            Collections.emptyList()));
        }
    }
}
