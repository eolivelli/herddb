/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster.follower;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.codec.RecordSerializer;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for reading from follower replicas.
 */
public class FollowerReadsTest extends MultiServerBase {

    @Test
    public void testReadFromFollower() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_NODEID, "server1")
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER)
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout())
                .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false)
                .set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.STRING)
                    .primaryKey("c")
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1, "s", "value1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 2, "s", "value2")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // force BK LAC update so follower can see all entries
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 3, "s", "value3")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Add server2 as follower
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

            // Wait for follower to catch up - poll using GetStatement (proven pattern)
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(
                        new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(3), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }

            // Now test client with allowReadsFromFollowers=true
            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);

            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {
                // SELECT should succeed (may go to follower or leader)
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM t1 ORDER BY c", true, Collections.emptyList(), 0, 0, 10, false)) {
                    assertTrue(scan.hasNext());
                    Map<String, Object> row1 = scan.next().toMap();
                    assertNotNull(row1);
                    assertEquals(1, row1.get("c"));
                    assertTrue(scan.hasNext());
                    Map<String, Object> row2 = scan.next().toMap();
                    assertEquals(2, row2.get("c"));
                }

                // INSERT should succeed (goes to leader)
                connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO t1 (c,s) VALUES (4,'value4')", 0, false, true, Collections.emptyList());

                // Verify the insert worked by reading from leader (using a transaction to bypass follower reads)
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT count(*) as cnt FROM t1", true, Collections.emptyList(), 0, 0, 10, false)) {
                    assertTrue(scan.hasNext());
                }
            }
        }
    }

    @Test
    public void testFollowerFailoverToLeader() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_NODEID, "server1")
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER)
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout())
                .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false)
                .set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            Server server_2 = new Server(serverconfig_2);
            server_2.start();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.STRING)
                    .primaryKey("c")
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1, "s", "value1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // force BK LAC
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 2, "s", "value2")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Add server2 as follower
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

            // Wait for data to replicate
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(
                        new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(2), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }

            // Stop server2 (the follower)
            server_2.close();

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
            clientConfig.set(ClientConfiguration.PROPERTY_OPERATION_RETRY_DELAY, 100);
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_OPERATION_RETRY_COUNT, 20);

            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {

                // SELECT should still succeed - after retry it should reach the leader
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10, false)) {
                    assertTrue(scan.hasNext());
                    Map<String, Object> row1 = scan.next().toMap();
                    assertNotNull(row1);
                    assertEquals(1, row1.get("c"));
                }
            }
        }
    }

    @Test
    public void testNoFollowerReadsInTransaction() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_NODEID, "server1")
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER)
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout())
                .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false)
                .set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Add server2 as follower
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);

            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {
                // Begin a transaction
                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                assertTrue(tx > 0);

                // SELECT inside transaction should go to leader and work
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM t1", true, Collections.emptyList(), tx, 0, 10, false)) {
                    assertTrue(scan.hasNext());
                }

                connection.commitTransaction(TableSpace.DEFAULT, tx);
            }
        }
    }

    @Test
    public void testFollowerPromotedToLeader() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath())
                .set(ServerConfiguration.PROPERTY_NODEID, "server1")
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER)
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath())
                .set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout())
                .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false)
                .set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.STRING)
                    .primaryKey("c")
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1, "s", "value1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // force BK LAC
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 2, "s", "value2")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Add server2 as follower
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

            // Wait for data to replicate
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(
                        new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(2), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }

            // Promote server2 to leader
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            clientConfig.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
            clientConfig.set(ClientConfiguration.PROPERTY_OPERATION_RETRY_DELAY, 100);

            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {
                // SELECT should work with new topology
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10, false)) {
                    assertTrue(scan.hasNext());
                    Map<String, Object> row1 = scan.next().toMap();
                    assertNotNull(row1);
                    assertEquals(1, row1.get("c"));
                }

                // INSERT should work (goes to new leader server2)
                connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO t1 (c,s) VALUES (10,'value10')", 0, false, true, Collections.emptyList());
            }
        }
    }
}
