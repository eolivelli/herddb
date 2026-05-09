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
package herddb.indexing.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.index.vector.NewSegmentInfo;
import herddb.log.LogSequenceNumber;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Focused unit test for the {@code mapFileSize} round-trip through
 * {@link SegmentRegistryPublisher} → ZooKeeper znode → {@link SegmentMetadata}
 * (issue #484, round-2 review item B.1.1).
 *
 * <p>The optimizer-side merger requires the EXACT map-file size to drive
 * the multipart reader; previously the publisher built {@code SegmentMetadata}
 * without populating the new {@code mapFileSize} field, causing every merge
 * to fall through to a broken legacy fallback. This test exercises the
 * ladder synthetically — feeding a {@link NewSegmentInfo} with a known
 * {@code mapFileSize} into {@code commitStagedSegments}, then reading the
 * znode back via {@link SegmentRegistryClient} and asserting the field
 * round-tripped intact.
 */
public class SegmentRegistryPublisherMapFileSizeTest {

    private static final String BASE_PATH = "/herd-test-mapfilesize";
    private static final String TS = "ts-mfs";
    private static final String TBL = "docs";
    private static final String IDX = "idx-mfs";
    private static final String IDX_NAME = "docs_v1";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();
    }

    @After
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    @Test
    public void publishedMetadataIncludesMapFileSize() throws Exception {
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS, TBL, IDX, IDX_NAME, /* instanceId */ 0);

        long expectedMapSize = 12_345_678L;
        NewSegmentInfo info = new NewSegmentInfo(
                /* segmentId */ 42,
                /* segmentUuid */ "stable-segment-uuid",
                /* graphPath */ "graph/path/x",
                /* graphFileSize */ 99_999L,
                /* mapPath */ "map/path/x",
                /* mapFileSize */ expectedMapSize,
                /* estimatedSizeBytes */ 99_999L + expectedMapSize,
                /* vectorCount */ 1234L,
                /* generation */ 7L,
                /* baseLsn */ new LogSequenceNumber(1L, 100L));

        publisher.commitStagedSegments(Collections.singletonList(info));

        List<VersionedSegmentMetadata> registered = registry.listSegments(TS, IDX);
        assertEquals(1, registered.size());
        SegmentMetadata m = registered.get(0).metadata();
        assertEquals("mapFileSize must round-trip from NewSegmentInfo through the znode",
                expectedMapSize, m.getMapFileSize());
        assertEquals("graphFileSize-derived sizeBytes hint must round-trip too",
                99_999L + expectedMapSize, m.getSizeBytes());
        assertEquals(42L, m.getSegmentId());
    }

    @Test
    public void provisionalSegmentAlsoCarriesMapFileSize() throws Exception {
        // The PROVISIONAL → ACTIVE staging path also goes through buildMetadata;
        // verify both use the same code path so PROVISIONAL znodes ALSO carry
        // mapFileSize. Without this, a stage/commit cycle could lose the field
        // on the way through.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS, TBL, IDX, IDX_NAME, 0);

        long expectedMapSize = 5_555_555L;
        NewSegmentInfo info = new NewSegmentInfo(
                /* segmentId */ 7, "uuid-prov",
                "g", 100L, "m", expectedMapSize,
                100L + expectedMapSize, 99L, 1L, new LogSequenceNumber(1L, 50L));

        publisher.stageNewSegments(Collections.singletonList(info));

        List<VersionedSegmentMetadata> registered = registry.listSegments(TS, IDX);
        assertEquals(1, registered.size());
        SegmentMetadata m = registered.get(0).metadata();
        assertEquals("PROVISIONAL state expected", SegmentState.PROVISIONAL, m.getState());
        assertEquals("PROVISIONAL znode must also carry mapFileSize",
                expectedMapSize, m.getMapFileSize());
    }
}
