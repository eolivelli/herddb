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
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Validates fix D4: {@link SegmentRegistryClient} retries ZK operations that
 * fail with {@link KeeperException.ConnectionLossException}, up to a bounded
 * number of attempts. Uses a {@link FaultInjectingZk} that wraps the real ZK
 * client and throws {@code ConnectionLossException} from {@code getData} the
 * first N times before delegating.
 */
public class SegmentRegistryConnectionLossRetryTest {

    private static final String BASE_PATH = "/herd-test-D4";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper realZk;
    private FaultInjectingZk faultyZk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        realZk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        realZk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        faultyZk = new FaultInjectingZk(realZk);
        registry = new SegmentRegistryClient(() -> faultyZk, BASE_PATH);
        registry.ensureRoot();
    }

    @After
    public void tearDown() throws Exception {
        if (realZk != null) {
            realZk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private SegmentMetadata sampleSegment(String segUuid) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid).tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void getSegmentRecoversFromTransientConnectionLoss() throws Exception {
        registry.createSegment(sampleSegment("seg-A"));

        // First 2 calls to getData will throw ConnectionLossException; the 3rd succeeds.
        faultyZk.getDataFailures.set(2);
        java.util.Optional<VersionedSegmentMetadata> v = registry.getSegment(TS_UUID, IDX_UUID, "seg-A");
        assertTrue(v.isPresent());
        assertEquals("seg-A", v.get().metadata().getSegmentUuid());
        assertEquals("retry counter must show 2 retries before success",
                0, faultyZk.getDataFailures.get());
    }

    @Test
    public void getSegmentEventuallyFailsIfFailuresExceedRetries() throws Exception {
        registry.createSegment(sampleSegment("seg-B"));

        // Always fail — the retry loop must give up after CONNECTION_LOSS_RETRIES attempts.
        faultyZk.getDataFailures.set(Integer.MAX_VALUE);
        try {
            registry.getSegment(TS_UUID, IDX_UUID, "seg-B");
            fail("expected SegmentRegistryException after exhausting retries");
        } catch (SegmentRegistryException ok) {
            // The last attempt's ConnectionLossException is wrapped.
            Throwable root = ok.getCause();
            assertTrue("root cause should be ConnectionLossException, was " + root,
                    root instanceof KeeperException.ConnectionLossException);
        }
    }

    @Test
    public void listSegmentsRetriesOnConnectionLoss() throws Exception {
        registry.createSegment(sampleSegment("seg-A"));
        registry.createSegment(sampleSegment("seg-B"));

        faultyZk.getChildrenFailures.set(1);
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals(2, all.size());
    }

    @Test
    public void casUpdateRetriesOnConnectionLoss() throws Exception {
        registry.createSegment(sampleSegment("seg-cas"));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-cas").orElseThrow();

        faultyZk.setDataFailures.set(2);
        VersionedSegmentMetadata updated = registry.casUpdateSegment(current,
                current.metadata().toBuilder().state(SegmentState.TRANSFERRING).build());
        assertEquals(SegmentState.TRANSFERRING, updated.metadata().getState());
    }

    @Test
    public void createSegmentRetriesOnConnectionLoss() throws Exception {
        // First create another segment so the parent znode chain (tablespace + index)
        // is already in place. The createFailures counter is gated by path so it only
        // fails the segment-leaf create, not the parent-chain idempotent ensures.
        registry.createSegment(sampleSegment("seg-warmup"));

        faultyZk.createFailureSegmentPathSuffix = "seg-create";
        faultyZk.createFailures.set(1);
        registry.createSegment(sampleSegment("seg-create"));
        java.util.Optional<VersionedSegmentMetadata> v = registry.getSegment(TS_UUID, IDX_UUID, "seg-create");
        assertTrue(v.isPresent());
    }

    /**
     * Wraps a real {@link ZooKeeper} but lets the test inject {@link KeeperException.ConnectionLossException}
     * on a controllable subset of operations. We only override the handful of methods the
     * registry actually calls.
     */
    private static final class FaultInjectingZk extends ZooKeeper {
        final AtomicInteger getDataFailures = new AtomicInteger();
        final AtomicInteger getChildrenFailures = new AtomicInteger();
        final AtomicInteger setDataFailures = new AtomicInteger();
        final AtomicInteger createFailures = new AtomicInteger();
        /** Optional suffix filter — when set, only paths ending with this suffix consume {@code createFailures}. */
        volatile String createFailureSegmentPathSuffix = null;

        private final ZooKeeper delegate;

        FaultInjectingZk(ZooKeeper delegate) throws IOException {
            super(delegate.getTestable() != null ? "127.0.0.1:1" : "127.0.0.1:1",
                    30000, e -> { });
            // ZooKeeper has no public copy constructor. Closing this auxiliary client
            // immediately so it doesn't try to maintain a real connection — we delegate
            // every call to the supplied real client.
            try {
                super.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.delegate = delegate;
        }

        private boolean shouldFail(AtomicInteger counter) {
            int remaining = counter.get();
            if (remaining <= 0) {
                return false;
            }
            counter.decrementAndGet();
            return true;
        }

        @Override
        public byte[] getData(String path, Watcher watcher, Stat stat)
                throws KeeperException, InterruptedException {
            if (shouldFail(getDataFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.getData(path, watcher, stat);
        }

        @Override
        public byte[] getData(String path, boolean watch, Stat stat)
                throws KeeperException, InterruptedException {
            if (shouldFail(getDataFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.getData(path, watch, stat);
        }

        @Override
        public List<String> getChildren(String path, Watcher watcher)
                throws KeeperException, InterruptedException {
            if (shouldFail(getChildrenFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.getChildren(path, watcher);
        }

        @Override
        public List<String> getChildren(String path, boolean watch)
                throws KeeperException, InterruptedException {
            if (shouldFail(getChildrenFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.getChildren(path, watch);
        }

        @Override
        public Stat setData(String path, byte[] data, int version)
                throws KeeperException, InterruptedException {
            if (shouldFail(setDataFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.setData(path, data, version);
        }

        @Override
        public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
                throws KeeperException, InterruptedException {
            String suffix = createFailureSegmentPathSuffix;
            boolean pathMatch = (suffix == null) || path.endsWith(suffix);
            if (pathMatch && shouldFail(createFailures)) {
                throw new KeeperException.ConnectionLossException();
            }
            return delegate.create(path, data, acl, createMode);
        }

        @Override
        public Stat exists(String path, Watcher watcher)
                throws KeeperException, InterruptedException {
            return delegate.exists(path, watcher);
        }

        @Override
        public Stat exists(String path, boolean watch)
                throws KeeperException, InterruptedException {
            return delegate.exists(path, watch);
        }

        @Override
        public void delete(String path, int version)
                throws InterruptedException, KeeperException {
            delegate.delete(path, version);
        }
    }
}
