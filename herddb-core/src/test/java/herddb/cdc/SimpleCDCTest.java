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

package herddb.cdc;

import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.cluster.RetryOnLeaderChangedTest;
import herddb.codec.RecordSerializer;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.*;
import herddb.model.commands.*;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.IntHolder;
import herddb.utils.ZKTestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;

/**
 * Tests around backup/restore
 *
 * @author enrico.olivelli
 */
public class SimpleCDCTest {

    private static final Logger LOG = Logger.getLogger(SimpleCDCTest.class.getName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testCaptureDataChange() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            List<CaptureDataChange.Mutation> mutations = new ArrayList<>();
            try (final CaptureDataChange cdc = new CaptureDataChange(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    new CaptureDataChange.MutationListener() {
                        @Override
                        public void accept(CaptureDataChange.Mutation mutation) {
                            LOG.log(Level.INFO, "mutation "+mutation);
                            mutations.add(mutation);
                        }
                    },
                    LogSequenceNumber.START_OF_TIME);) {
                cdc.run();

                // we are missing the last entry, because it is not confirmed yet on BookKeeper at this point
                assertEquals(4, mutations.size());

            }
        }
    }

}
