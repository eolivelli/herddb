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
package herddb.indexing.optimizer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Monotonic per-tablespace fencing token stored as a single PERSISTENT znode
 * at {@code <basePath>/index-optimizer/leader-epoch-<tablespaceUuid>}. The
 * leader CAS-bumps the value at every producer tick start; a stale leader
 * whose CAS hits {@code BadVersionException} aborts the tick. The current
 * value is also written into every {@link OptimizerTask} so consumers and
 * future operators can attribute tasks to a specific leadership generation
 * (step 7 uses this).
 *
 * <p>Step 3 lands the primitive; the producer-side CAS bump call lands in
 * step 5 alongside the task-producer wiring.
 */
public final class LeaderEpoch {

    public static final String SUBPATH_PREFIX = "/index-optimizer/leader-epoch-";

    private final Supplier<ZooKeeper> zkSupplier;
    private final String optimizerRootPath;
    private final String path;

    public LeaderEpoch(Supplier<ZooKeeper> zkSupplier, String basePath, String tablespaceUuid) {
        this.zkSupplier = Objects.requireNonNull(zkSupplier, "zkSupplier");
        Objects.requireNonNull(basePath, "basePath");
        Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.optimizerRootPath = basePath + "/index-optimizer";
        this.path = basePath + SUBPATH_PREFIX + tablespaceUuid;
    }

    public String getPath() {
        return path;
    }

    /**
     * @return current epoch ({@code 0} when the znode does not yet exist).
     */
    public long currentEpoch() throws OptimizerTaskRegistryException {
        try {
            Stat stat = new Stat();
            byte[] data = zk().getData(path, false, stat);
            return parseEpoch(data);
        } catch (KeeperException.NoNodeException e) {
            return 0L;
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to read leader epoch from " + path, e);
        }
    }

    /**
     * Atomically increments the epoch and returns the new value. Concurrent
     * callers race on a single ZK CAS; the loser sees
     * {@code BadVersionException} surfaced as
     * {@link OptimizerTaskRegistryException.VersionMismatch} so the caller
     * (a stale leader) can abort its tick.
     */
    public long bumpEpoch() throws OptimizerTaskRegistryException {
        try {
            Stat stat = new Stat();
            byte[] data;
            try {
                data = zk().getData(path, false, stat);
            } catch (KeeperException.NoNodeException missing) {
                // First-ever bump: create the parent optimizer-root and the
                // epoch znode with value 1.
                ensureOptimizerRoot();
                try {
                    zk().create(path, encode(1L), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    return 1L;
                } catch (KeeperException.NodeExistsException raceLost) {
                    // Another bumper won the create race; re-read and retry the
                    // setData branch instead of returning a stale "1L".
                    data = zk().getData(path, false, stat);
                }
            }
            long next = parseEpoch(data) + 1L;
            zk().setData(path, encode(next), stat.getVersion());
            return next;
        } catch (KeeperException.BadVersionException e) {
            throw new OptimizerTaskRegistryException.VersionMismatch("leader-epoch", -1);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to bump leader epoch at " + path, e);
        }
    }

    private void ensureOptimizerRoot() throws KeeperException, InterruptedException {
        try {
            zk().create(optimizerRootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
            // expected on every call after the first
        }
    }

    private static long parseEpoch(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        return Long.parseLong(new String(data, StandardCharsets.UTF_8).trim());
    }

    private static byte[] encode(long value) {
        return Long.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    private ZooKeeper zk() {
        ZooKeeper zk = zkSupplier.get();
        if (zk == null) {
            throw new IllegalStateException("ZooKeeper supplier returned null");
        }
        return zk;
    }
}
