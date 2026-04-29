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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper-backed singleton lock for the index-optimizer service (review item
 * C2 — Helm {@code replicas: 1} alone is not enough during rolling upgrades).
 *
 * <p>Layout: a single ephemeral znode at {@code {basePath}/index-optimizer/leader-{tablespaceUuid}}
 * whose data is the holder's own ID (a UUID generated at construction). Only the
 * holder runs the optimizer's {@code runOnce()} ticks; standby pods that fail to
 * acquire skip the tick and retry on the next one. Failure modes:
 * <ul>
 *   <li>The current holder's ZK session expires → ephemeral znode disappears → next
 *       attempt by anyone wins.</li>
 *   <li>Two pods race during a rolling upgrade → ZK serialises the {@code create},
 *       only one wins the lock; the loser's {@code tryAcquire} returns false.</li>
 *   <li>Holder's ZK supplier returns a fresh client (e.g. after a session restart
 *       inside {@link herddb.indexing.optimizer.IndexOptimizerMain}) → the previous
 *       ephemeral znode is gone with the old session, so the new ZK can re-acquire.</li>
 * </ul>
 *
 * <p>This class is thread-safe. The {@link #tryAcquire} call is the only state
 * mutation; subsequent {@link #isHeld} checks just read the cached "we hold it"
 * boolean. There is no proactive watcher: callers re-check ownership on every
 * tick (see {@link IndexOptimizerEngine}-level wiring), which keeps the lock
 * machinery free of background threads.
 */
public final class OptimizerLeaderLock implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(OptimizerLeaderLock.class.getName());

    /**
     * Sub-path appended to the HerdDB base path for the optimizer's leader znode root.
     */
    public static final String LOCK_SUBPATH = "/index-optimizer";

    private final Supplier<ZooKeeper> zkSupplier;
    private final String lockRootPath;
    private final String lockPath;
    private final String holderId;

    /** Whether the local pod currently believes it is the leader. */
    private volatile boolean held;

    /**
     * @param zkSupplier     supplies a live {@link ZooKeeper} instance (must reuse across
     *                       reconnects so an ephemeral node tied to a stale session is
     *                       recognised as unowned).
     * @param basePath       HerdDB base ZK path (e.g. {@code /herd}).
     * @param tablespaceUuid the tablespace UUID this optimizer manages — different
     *                       tablespaces get separate locks so they can run on different
     *                       optimizer pods without contention.
     */
    public OptimizerLeaderLock(Supplier<ZooKeeper> zkSupplier, String basePath, String tablespaceUuid) {
        this.zkSupplier = Objects.requireNonNull(zkSupplier, "zkSupplier");
        Objects.requireNonNull(basePath, "basePath");
        Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.lockRootPath = basePath + LOCK_SUBPATH;
        this.lockPath = lockRootPath + "/leader-" + tablespaceUuid;
        this.holderId = UUID.randomUUID().toString();
    }

    public String getLockPath() {
        return lockPath;
    }

    public String getHolderId() {
        return holderId;
    }

    public boolean isHeld() {
        return held;
    }

    /** Ensures the parent path exists (idempotent, called lazily). */
    public void ensureRoot() throws KeeperException, InterruptedException {
        try {
            zk().create(lockRootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
            // benign — already created by an earlier optimizer pod
        }
    }

    /**
     * Attempts to become leader by creating the ephemeral lock znode. Returns
     * {@code true} if we now hold the lock (newly acquired or already held), or
     * {@code false} if another pod owns it.
     *
     * <p>Side-effect-free on failure: a {@code false} return does not modify state.
     */
    public boolean tryAcquire() {
        if (held) {
            // Verify we still hold it — the ephemeral may have disappeared if our
            // session expired. If it's gone, fall through to re-acquire.
            try {
                Stat st = zk().exists(lockPath, false);
                if (st != null && holderId.equals(readHolderId(zk(), lockPath))) {
                    return true;
                }
                held = false;
            } catch (KeeperException | InterruptedException | IOException check) {
                if (check instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                LOGGER.log(Level.WARNING,
                        "tryAcquire could not verify current holder; treating as not-held: {0}",
                        check.getMessage());
                held = false;
            }
        }
        try {
            ensureRoot();
            zk().create(lockPath, holderId.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            held = true;
            LOGGER.log(Level.INFO, "optimizer leader lock acquired: {0} ({1})",
                    new Object[]{lockPath, holderId});
            return true;
        } catch (KeeperException.NodeExistsException heldByOther) {
            // Inspect the current holder for visibility.
            try {
                String currentHolder = readHolderId(zk(), lockPath);
                LOGGER.log(Level.FINE,
                        "optimizer leader lock held by {0}; we ({1}) will retry on next tick",
                        new Object[]{currentHolder, holderId});
            } catch (Exception ignored) {
                // not fatal
            }
            return false;
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(Level.WARNING, "tryAcquire failed: {0}", e.getMessage());
            return false;
        }
    }

    /**
     * Voluntary release. Best-effort: a session expiry will clean up the ephemeral
     * znode automatically; this method is for graceful shutdown.
     */
    public void release() {
        if (!held) {
            return;
        }
        try {
            // Verify we still own it before deleting (defensive).
            Stat st = zk().exists(lockPath, false);
            if (st == null) {
                held = false;
                return;
            }
            String currentHolder = readHolderId(zk(), lockPath);
            if (!holderId.equals(currentHolder)) {
                LOGGER.log(Level.WARNING,
                        "release: lock at {0} held by {1}, not us ({2}); skipping delete",
                        new Object[]{lockPath, currentHolder, holderId});
                held = false;
                return;
            }
            zk().delete(lockPath, st.getVersion());
            held = false;
            LOGGER.log(Level.INFO, "optimizer leader lock released: {0}", lockPath);
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(Level.WARNING, "release failed: {0}", e.getMessage());
        }
    }

    @Override
    public void close() {
        release();
    }

    private static String readHolderId(ZooKeeper zk, String path)
            throws KeeperException, InterruptedException, IOException {
        try {
            byte[] data = zk.getData(path, false, null);
            return data == null ? "" : new String(data, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException gone) {
            return "";
        }
    }

    private ZooKeeper zk() {
        ZooKeeper zk = zkSupplier.get();
        if (zk == null) {
            throw new IllegalStateException("ZooKeeper supplier returned null");
        }
        return zk;
    }
}
