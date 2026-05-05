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
import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test (issue #398): a {@link PromotableRemoteFileDataStorageManager}
 * promoted to writable mode must honour the configurable cleanup batch size and
 * register the cleanup metrics on the supplied {@link TestStatsProvider} —
 * otherwise post-failover boot-cleanup activity is silently dropped from
 * Prometheus, which is exactly the failover scenario where it matters most.
 */
public class PromotableRemoteFileDataStorageManagerCleanupBatchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private TestStatsProvider statsProvider;
    private PromotableRemoteFileDataStorageManager promotable;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        server.start();
        client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()));
        statsProvider = new TestStatsProvider();
        SharedCheckpointMetadataManager metadataManager = new SharedCheckpointMetadataManager(client);
        ReadReplicaDataStorageManager readReplica = new ReadReplicaDataStorageManager(
                client, metadataManager, folder.newFolder("tmp").toPath(), 1000);
        promotable = new PromotableRemoteFileDataStorageManager(
                readReplica, client, metadataManager,
                folder.newFolder("data").toPath(),
                folder.newFolder("tmp2").toPath(),
                1000,
                herddb.server.ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT,
                herddb.server.ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT,
                /* cleanupBatchSize */ 7,
                statsProvider.getStatsLogger("test"));
        promotable.start();
    }

    @After
    public void tearDown() throws Exception {
        if (promotable != null) {
            promotable.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void promotedManagerHonoursBatchSizeAndPublishesMetrics() throws Exception {
        // Promote first so the writable delegate is wired up; the cleanup metric
        // counters must be registered against the StatsLogger we passed to the
        // promotable wrapper, NOT against a NullStatsLogger silently substituted
        // somewhere along the chain.
        promotable.promoteToWritable();

        promotable.initTablespace("ts1");
        promotable.initTable("ts1", "uuid1");
        for (int p = 1; p <= 30; p++) {
            promotable.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }
        Set<Long> active = new HashSet<>(Arrays.asList(5L, 17L));
        // 30 pages, 2 active → 28 stale. Batch size 7 → ceil(28/7) = 4 batches.
        promotable.cleanupAfterTableBoot("ts1", "uuid1", active);

        Counter deletions = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("deletions");
        Counter batches = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("batches");
        assertEquals("28 stale pages must be reported deleted on the promoted manager",
                28, deletions.get().intValue());
        assertEquals("28 / 7 = 4 batches expected",
                4, batches.get().intValue());
    }
}
