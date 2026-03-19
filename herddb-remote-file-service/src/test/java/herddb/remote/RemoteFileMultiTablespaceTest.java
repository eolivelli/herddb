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
 * Multi-tablespace test with 2 RemoteFileServer instances.
 * Verifies recovery of both tablespaces with a BRIN index.
 *
 * @author enrico.olivelli
 */
public class RemoteFileMultiTablespaceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testTwoTablespacesTwoServers() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpPath = folder.newFolder("tmp").toPath();

        String nodeId = "localhost";

        try (RemoteFileServer server1 = new RemoteFileServer(0, folder.newFolder("remote1").toPath());
             RemoteFileServer server2 = new RemoteFileServer(0, folder.newFolder("remote2").toPath())) {

            server1.start();
            server2.start();

            String addr1 = "localhost:" + server1.getPort();
            String addr2 = "localhost:" + server2.getPort();

            // First run: create 2 tablespaces, 1 table each, BRIN index on ts1
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr1, addr2));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {

                manager.start();

                for (String ts : Arrays.asList("tblspace1", "tblspace2")) {
                    manager.executeStatement(
                            new CreateTableSpaceStatement(ts, Collections.singleton(nodeId), nodeId, 1, 0, 0),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);
                    assertTrue(manager.waitForTablespace(ts, 10000));

                    Table table = Table.builder()
                            .tablespace(ts)
                            .name("t1")
                            .column("id", ColumnTypes.STRING)
                            .column("name", ColumnTypes.STRING)
                            .primaryKey("id")
                            .build();
                    manager.executeStatement(new CreateTableStatement(table),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);

                    RemoteTestUtils.executeUpdate(manager, "INSERT INTO " + ts + ".t1(id,name) values('a','n1')", Collections.emptyList());
                    RemoteTestUtils.executeUpdate(manager, "INSERT INTO " + ts + ".t1(id,name) values('b','n2')", Collections.emptyList());
                    RemoteTestUtils.executeUpdate(manager, "INSERT INTO " + ts + ".t1(id,name) values('c','n1')", Collections.emptyList());
                }

                // BRIN index only on tblspace1
                Table tbl1 = Table.builder().tablespace("tblspace1").name("t1")
                        .column("id", ColumnTypes.STRING).column("name", ColumnTypes.STRING)
                        .primaryKey("id").build();
                Index index = Index.builder().onTable(tbl1).type(Index.TYPE_BRIN)
                        .column("name", ColumnTypes.STRING).build();
                manager.executeStatement(new CreateIndexStatement(index),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);

                manager.checkpoint();
            }

            // Second run: reopen, verify both tablespaces recovered, BRIN index works
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr1, addr2));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {

                manager.start();
                assertTrue(manager.waitForBootOfLocalTablespaces(10000));

                // Verify tblspace1 with BRIN index
                TranslatedQuery q1 = manager.getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * FROM tblspace1.t1 WHERE name='n1'",
                        Collections.emptyList(), true, true, false, -1);
                ScanStatement scan1 = q1.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan1.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner ds = manager.scan(scan1, q1.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(2, ds.consume().size());
                }

                // Verify tblspace2 data recovered
                try (DataScanner ds = RemoteTestUtils.scan(manager, "SELECT * FROM tblspace2.t1", Collections.emptyList())) {
                    assertEquals(3, ds.consume().size());
                }
            }
        }
    }
}
