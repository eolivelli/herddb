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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the ZooKeeper-backed segment registry: create / read / list / CAS update /
 * CAS delete, including concurrent-update semantics and watcher notifications.
 *
 * <p>Uses curator-test {@code TestingServer} (no BookKeeper, no full ZKTestEnv) so the
 * tests are fast and only exercise the registry client itself.
 */
public class SegmentRegistryClientTest {

    private static final String BASE_PATH = "/herd-test-segments";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
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
    public void registryRootIsCreatedUnderBasePath() throws Exception {
        assertEquals(BASE_PATH + SegmentRegistryClient.REGISTRY_SUBPATH, registry.getRegistryRootPath());
        assertNotNull(zk.exists(registry.getRegistryRootPath(), false));
    }

    @Test
    public void createAndGetSegmentRoundtrip() throws Exception {
        SegmentMetadata segment = sampleSegment("seg-1", 0);
        registry.createSegment(segment);

        Optional<VersionedSegmentMetadata> found = registry.getSegment(TS_UUID, IDX_UUID, "seg-1");
        assertTrue(found.isPresent());
        assertEquals(segment, found.get().metadata());
        assertEquals(0, found.get().zkVersion());
    }

    @Test
    public void getMissingSegmentReturnsEmpty() throws Exception {
        Optional<VersionedSegmentMetadata> found = registry.getSegment(TS_UUID, IDX_UUID, "no-such-seg");
        assertFalse(found.isPresent());
    }

    @Test
    public void createSegmentTwiceThrowsAlreadyExists() throws Exception {
        SegmentMetadata segment = sampleSegment("seg-1", 0);
        registry.createSegment(segment);
        try {
            registry.createSegment(segment);
            fail("expected SegmentAlreadyExists");
        } catch (SegmentRegistryException.SegmentAlreadyExists ok) {
            assertTrue(ok.getMessage().contains("seg-1"));
        }
    }

    @Test
    public void listSegmentsReturnsAllUnderIndex() throws Exception {
        registry.createSegment(sampleSegment("seg-a", 0));
        registry.createSegment(sampleSegment("seg-b", 1));
        registry.createSegment(sampleSegment("seg-c", 0));

        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals(3, all.size());
        // Verify segments are correctly addressable by uuid in returned set.
        assertTrue(all.stream().anyMatch(v -> v.metadata().getSegmentUuid().equals("seg-a")));
        assertTrue(all.stream().anyMatch(v -> v.metadata().getSegmentUuid().equals("seg-b")));
        assertTrue(all.stream().anyMatch(v -> v.metadata().getSegmentUuid().equals("seg-c")));
    }

    @Test
    public void listSegmentsReturnsEmptyForUnknownIndex() throws Exception {
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, "nope");
        assertEquals(Collections.emptyList(), all);
    }

    @Test
    public void listIndexesAndTablespaces() throws Exception {
        registry.createSegment(sampleSegment("seg-a", 0));

        List<String> tablespaces = registry.listTablespaces();
        assertEquals(Collections.singletonList(TS_UUID), tablespaces);

        List<String> indexes = registry.listIndexes(TS_UUID);
        assertEquals(Collections.singletonList(IDX_UUID), indexes);
    }

    @Test
    public void casUpdateSucceedsWithMatchingVersion() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();
        SegmentMetadata transferring = current.metadata().toBuilder()
                .state(SegmentState.TRANSFERRING)
                .pendingOwnerInstanceId(1)
                .build();
        VersionedSegmentMetadata after = registry.casUpdateSegment(current, transferring);

        assertEquals(SegmentState.TRANSFERRING, after.metadata().getState());
        assertEquals(1, after.metadata().getPendingOwnerInstanceId());
        assertEquals(current.zkVersion() + 1, after.zkVersion());
    }

    @Test
    public void casUpdateFailsWithStaleVersion() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);

        VersionedSegmentMetadata staleA = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();
        VersionedSegmentMetadata staleB = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();

        // First CAS wins.
        registry.casUpdateSegment(staleA,
                staleA.metadata().toBuilder().state(SegmentState.TRANSFERRING).build());

        // Second CAS, holding the now-stale version, must fail.
        try {
            registry.casUpdateSegment(staleB,
                    staleB.metadata().toBuilder().state(SegmentState.DEPRECATED).build());
            fail("expected VersionMismatch");
        } catch (SegmentRegistryException.VersionMismatch ok) {
            assertTrue(ok.getMessage().contains("seg-1"));
        }
    }

    @Test
    public void casUpdateAddressingKeysFailsFast() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();
        SegmentMetadata withChangedKeys = current.metadata().toBuilder().segmentUuid("seg-2").build();
        try {
            registry.casUpdateSegment(current, withChangedKeys);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("addressing keys"));
        }
    }

    @Test
    public void casUpdateOnMissingSegmentThrowsNotFound() throws Exception {
        SegmentMetadata phantom = sampleSegment("seg-x", 0);
        VersionedSegmentMetadata fake = new VersionedSegmentMetadata(phantom, 0);
        try {
            registry.casUpdateSegment(fake, phantom.toBuilder().generation(2L).build());
            fail("expected SegmentNotFound");
        } catch (SegmentRegistryException.SegmentNotFound ok) {
            assertTrue(ok.getMessage().contains("seg-x"));
        }
    }

    @Test
    public void casDeleteSucceedsAndIdempotentlyCreatesEmptyList() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();
        registry.casDeleteSegment(current);

        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-1").isPresent());
        assertEquals(Collections.emptyList(), registry.listSegments(TS_UUID, IDX_UUID));
    }

    @Test
    public void casDeleteWithStaleVersionThrowsVersionMismatch() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);

        VersionedSegmentMetadata stale = registry.getSegment(TS_UUID, IDX_UUID, "seg-1").orElseThrow();
        // Bump the version by performing an update first.
        registry.casUpdateSegment(stale, stale.metadata().toBuilder().generation(2L).build());

        try {
            registry.casDeleteSegment(stale);
            fail("expected VersionMismatch");
        } catch (SegmentRegistryException.VersionMismatch ok) {
            assertTrue(ok.getMessage().contains("seg-1"));
        }
    }

    @Test
    public void getSegmentArmsDataWatcher() throws Exception {
        SegmentMetadata original = sampleSegment("seg-1", 0);
        registry.createSegment(original);

        CountDownLatch fired = new CountDownLatch(1);
        AtomicReference<Watcher.Event.EventType> seen = new AtomicReference<>();
        Watcher watcher = (WatchedEvent event) -> {
            seen.set(event.getType());
            fired.countDown();
        };

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-1", watcher).orElseThrow();
        registry.casUpdateSegment(current, current.metadata().toBuilder().generation(99L).build());

        assertTrue("watcher did not fire", fired.await(10, TimeUnit.SECONDS));
        assertSame(Watcher.Event.EventType.NodeDataChanged, seen.get());
    }

    @Test
    public void listSegmentsArmsChildrenWatcher() throws Exception {
        registry.createSegment(sampleSegment("seg-a", 0));

        CountDownLatch fired = new CountDownLatch(1);
        AtomicReference<Watcher.Event.EventType> seen = new AtomicReference<>();
        Watcher watcher = (WatchedEvent event) -> {
            seen.set(event.getType());
            fired.countDown();
        };

        registry.listSegments(TS_UUID, IDX_UUID, watcher);
        // Add a new segment under the same index — watcher should fire on children changed.
        registry.createSegment(sampleSegment("seg-b", 1));

        assertTrue("children watcher did not fire", fired.await(10, TimeUnit.SECONDS));
        assertSame(Watcher.Event.EventType.NodeChildrenChanged, seen.get());
    }

    @Test
    public void parentChainIsLazilyCreated() throws Exception {
        // Even with a fresh tablespace/index, createSegment must succeed.
        String freshTs = "ts-" + UUID.randomUUID();
        String freshIdx = "idx-" + UUID.randomUUID();
        SegmentMetadata segment = SegmentMetadata.builder()
                .segmentUuid("seg-fresh")
                .tablespaceUuid(freshTs)
                .tableName("t")
                .indexUuid(freshIdx)
                .indexName("i")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .build();
        registry.createSegment(segment);

        assertNotNull(zk.exists(registry.tablespacePath(freshTs), false));
        assertNotNull(zk.exists(registry.indexPath(freshTs, freshIdx), false));
        assertNotNull(zk.exists(registry.segmentPath(freshTs, freshIdx, "seg-fresh"), false));
    }

    private static SegmentMetadata sampleSegment(String segmentUuid, int owner) {
        return SegmentMetadata.builder()
                .segmentUuid(segmentUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(owner)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .graphPath(TS_UUID + "/" + IDX_UUID + "_" + segmentUuid + "/graph")
                .mapPath(TS_UUID + "/" + IDX_UUID + "_" + segmentUuid + "/map")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(1_700_000_000L)
                .replacedBy(Arrays.asList())
                .build();
    }
}
