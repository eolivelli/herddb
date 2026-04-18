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

package herddb.cluster;

import static org.junit.Assert.assertEquals;
import herddb.server.ServerConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Unit tests for the {@link ClientConfiguration} built by
 * {@link BookkeeperCommitLogManager}.
 *
 * <p>Focuses on the BK client timeout defaults introduced for issue #152.
 */
public class BookkeeperCommitLogManagerConfigTest {

    private static ZookeeperMetadataStorageManager fakeMetadata() {
        // The constructor of ZookeeperMetadataStorageManager does not open a ZK
        // connection — it only stores the values. BookkeeperCommitLogManager's
        // constructor likewise just reads these getters, so we can build the
        // manager without any real infrastructure.
        return new ZookeeperMetadataStorageManager("localhost:2181", 30_000, "/herddb-test");
    }

    @Test
    public void appliesDefaultAddEntryTimeouts() {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (BookkeeperCommitLogManager manager = new BookkeeperCommitLogManager(
                fakeMetadata(), serverConfiguration, NullStatsLogger.INSTANCE)) {
            ClientConfiguration config = manager.getClientConfiguration();
            assertEquals(BookkeeperCommitLogManager.DEFAULT_ADD_ENTRY_TIMEOUT_SEC,
                    config.getAddEntryTimeout());
            assertEquals(BookkeeperCommitLogManager.DEFAULT_ADD_ENTRY_QUORUM_TIMEOUT_SEC,
                    config.getAddEntryQuorumTimeout());
        }
    }

    @Test
    public void explicitBookkeeperPropertiesOverrideDefaults() {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.set("bookkeeper.addEntryTimeoutSec", "11");
        serverConfiguration.set("bookkeeper.addEntryQuorumTimeoutSec", "22");
        try (BookkeeperCommitLogManager manager = new BookkeeperCommitLogManager(
                fakeMetadata(), serverConfiguration, NullStatsLogger.INSTANCE)) {
            ClientConfiguration config = manager.getClientConfiguration();
            assertEquals(11, config.getAddEntryTimeout());
            assertEquals(22, config.getAddEntryQuorumTimeout());
        }
    }
}
