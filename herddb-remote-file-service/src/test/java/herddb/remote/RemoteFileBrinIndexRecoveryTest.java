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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.SecondaryIndexSeek;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Port of SimpleBrinIndexRecoveryTest using RemoteFileDataStorageManager.
 * Tests that DBManager lifecycle (create tablespace → insert → checkpoint → restart → query)
 * works correctly with remote page storage.
 *
 * @author enrico.olivelli
 */
public class RemoteFileBrinIndexRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBrinIndexCreateAndRecovery() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpPath = folder.newFolder("tmp").toPath();
        Path remotePath = folder.newFolder("remote").toPath();

        String nodeId = "localhost";

        try (RemoteFileServer server = new RemoteFileServer(0, remotePath);) {
            server.start();
            String serverAddr = "localhost:" + server.getPort();

            // First run: create table, BRIN index, insert data, checkpoint
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(serverAddr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {

                manager.start();
                CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                        Collections.singleton(nodeId), nodeId, 1, 0, 0);
                manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                assertTrue(manager.waitForTablespace("tblspace1", 10000));

                Table table = Table.builder()
                        .tablespace("tblspace1")
                        .name("t1")
                        .column("id", ColumnTypes.STRING)
                        .column("name", ColumnTypes.STRING)
                        .primaryKey("id")
                        .build();
                manager.executeStatement(new CreateTableStatement(table),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);

                Index index = Index.builder()
                        .onTable(table)
                        .type(Index.TYPE_BRIN)
                        .column("name", ColumnTypes.STRING)
                        .build();

                RemoteTestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
                RemoteTestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
                RemoteTestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
                RemoteTestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
                RemoteTestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

                manager.executeStatement(new CreateIndexStatement(index),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);

                TranslatedQuery q = manager.getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * FROM tblspace1.t1 WHERE name='n1'",
                        Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = q.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner ds = manager.scan(scan, q.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(3, ds.consume().size());
                }

                manager.checkpoint();
            }

            // Second run: reopen, verify data + BRIN index recovered
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(serverAddr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {

                manager.start();
                assertTrue(manager.waitForBootOfLocalTablespaces(10000));

                TranslatedQuery q = manager.getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * FROM tblspace1.t1 WHERE name='n1'",
                        Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = q.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner ds = manager.scan(scan, q.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(3, ds.consume().size());
                }
            }
        }
    }
}
