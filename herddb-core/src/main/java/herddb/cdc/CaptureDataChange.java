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

import java.util.HashMap;
import java.util.Map;

import herddb.client.ClientConfiguration;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.DataAccessorForFullRecord;
import herddb.codec.RecordSerializer;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Record;
import herddb.model.Table;
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
        DELETE,
        CREATETABLE,
        DELETETABLE,
        ALTERTABLE
    }

    public static class Mutation {
        private final Table table;
        private final MutationType mutationType;
        private final DataAccessorForFullRecord record;
        private final LogSequenceNumber logSequenceNumber;
        private final long timestamp;

        public Mutation(Table table, MutationType mutationType,
                DataAccessorForFullRecord record, LogSequenceNumber logSequenceNumber,
                long timestamp)
        {
            this.table = table;
            this.mutationType = mutationType;
            this.record = record;
            this.logSequenceNumber = logSequenceNumber;
            this.timestamp = timestamp;
        }

        public Table getTable()
        {
            return table;
        }

        public MutationType getMutationType()
        {
            return mutationType;
        }

        public DataAccessorForFullRecord getRecord()
        {
            return record;
        }

        public LogSequenceNumber getLogSequenceNumber()
        {
            return logSequenceNumber;
        }

        public long getTimestamp()
        {
            return timestamp;
        }
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

    private Map<String, Table> tablesDefinition = new HashMap<>();

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
                        applyEntry(entry, lsn);
                        return !closed;
                    }
                }, context);
            }
        }
    }
    public void close() {
        closed = true;
    }

    private void applyEntry(LogEntry entry, LogSequenceNumber lsn) throws Exception
    {
        switch (entry.type)
        {
            case LogEntryType.NOOP:
            case LogEntryType.CREATE_INDEX:
            case LogEntryType.DROP_INDEX:
                break;
            case LogEntryType.DROP_TABLE:
            {
                Table table = tablesDefinition.remove(entry.tableName);
                listener.accept(new Mutation(table, MutationType.DELETETABLE, null, lsn, entry.timestamp));
            }
            ;
            break;
            case LogEntryType.CREATE_TABLE:
            {
                Table table = Table.deserialize(entry.value.to_array());
                tablesDefinition.put(entry.tableName, table);
                listener.accept(new Mutation(table, MutationType.CREATETABLE, null, lsn, entry.timestamp));
            }
            ;
            break;
            case LogEntryType.INSERT:
            {
                Table table = tablesDefinition.get(entry.tableName);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                listener.accept(new Mutation(table, MutationType.CREATETABLE, record, lsn, entry.timestamp));
            }
            ;
            break;
            case LogEntryType.DELETE:
            {
                Table table = tablesDefinition.get(entry.tableName);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                listener.accept(new Mutation(table, MutationType.DELETE, record, lsn, entry.timestamp));
            }
            ;
            break;
            case LogEntryType.UPDATE:
            {
                Table table = tablesDefinition.get(entry.tableName);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                listener.accept(new Mutation(table, MutationType.UPDATE, record, lsn, entry.timestamp));
            }
            ;
            break;
            case LogEntryType.BEGINTRANSACTION:
            case LogEntryType.COMMITTRANSACTION:
            case LogEntryType.ROLLBACKTRANSACTION:
                break;
            default:
                // discard unkwown entry types
                break;
        }
    }
    private static ZookeeperMetadataStorageManager buildMetadataStorageManager(ClientConfiguration configuration)
        throws MetadataStorageManagerException
    {
        String zkAddress = configuration.getString(PROPERTY_ZOOKEEPER_ADDRESS, PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        String zkPath = configuration.getString(PROPERTY_ZOOKEEPER_PATH, PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        int sessionTimeout = configuration.getInt(PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 60000);
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, zkPath);
        zk.start(false);
        return zk;
    }
}
