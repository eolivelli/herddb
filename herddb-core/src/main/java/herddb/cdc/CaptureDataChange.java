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

import java.util.Map;

import herddb.client.ClientConfiguration;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManagerException;
import herddb.server.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;

import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT;

public class CaptureDataChange implements AutoCloseable {

    public enum MutationType {
        INSERT,
        UPDATE,
        DELETE
    }

    public interface Mutation {
        String getTableNane();
        String getTableSpaceName();
        MutationType getMutationType();
        Map<String, Object> getPrimaryKey();
        Map<String, Object> getValue();
        LogSequenceNumber getLogSequenceNumber();
    }

    public interface MutationListener {
        void accept(Mutation mutation);
    }

    private final ClientConfiguration configuration;
    private final MutationListener listener;
    private final LogSequenceNumber startingPosition;
    private final String tableSpaceUUID;
    private volatile boolean closed = false;

    public CaptureDataChange(String tableSpaceUUID, ClientConfiguration configuration, MutationListener listener, LogSequenceNumber startingPosition)
    {
        this.configuration = configuration;
        this.listener = listener;
        this.startingPosition = startingPosition;
        this.tableSpaceUUID = tableSpaceUUID;
    }

    public void run() throws Exception {
        try (ZookeeperMetadataStorageManager zookeeperMetadataStorageManager = buildMetadataStorageManager(configuration);
             BookkeeperCommitLogManager manager = new BookkeeperCommitLogManager(zookeeperMetadataStorageManager, new ServerConfiguration(), NullStatsLogger.INSTANCE);)
        {
            manager.start();
            try (BookkeeperCommitLog cdc = manager.createCommitLog(tableSpaceUUID, tableSpaceUUID, "cdc");)
            {
                CommitLog.FollowerContext context = cdc.startFollowing(startingPosition);
                cdc.followTheLeader(startingPosition, new CommitLog.EntryAcceptor()
                {
                    @Override
                    public boolean accept(LogSequenceNumber lsn, LogEntry entry) throws Exception
                    {
                        return !closed;
                    }
                }, context);
            }
        }
    }
    public void close() {
        closed = true;
    }

    private static ZookeeperMetadataStorageManager buildMetadataStorageManager(ClientConfiguration configuration) throws MetadataStorageManagerException
    {
        String zkAddress = configuration.getString(PROPERTY_ZOOKEEPER_ADDRESS, PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        String zkPath = configuration.getString(PROPERTY_ZOOKEEPER_PATH, PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        int sessionTimeout = configuration.getInt(PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 60000);
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, zkPath);
        zk.start(false /*
         * formatIfNeeded
         */);
        return zk;
    }
}
