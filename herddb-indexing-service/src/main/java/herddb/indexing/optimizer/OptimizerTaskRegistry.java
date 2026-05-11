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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZK-backed CRUD + lease management for {@link OptimizerTask} records. Path
 * layout:
 *
 * <pre>
 *   {basePath}/index-optimizer/tasks/                        (PERSISTENT root)
 *   {basePath}/index-optimizer/tasks/{tablespaceUuid}/       (PERSISTENT per-tablespace dir)
 *   {basePath}/index-optimizer/tasks/{tablespaceUuid}/{taskId}        (PERSISTENT task znode)
 *   {basePath}/index-optimizer/tasks/{tablespaceUuid}/{taskId}/lease  (EPHEMERAL worker liveness)
 * </pre>
 *
 * <p>Mirrors the contract of
 * {@link herddb.indexing.segment.SegmentRegistryClient} — CAS updates via
 * znode version, retry-on-connection-loss, typed exceptions. Step 3 lands the
 * client; producer + consumer wiring is steps 5 / 6 / 7.
 */
public final class OptimizerTaskRegistry {

    private static final Logger LOGGER = Logger.getLogger(OptimizerTaskRegistry.class.getName());
    private static final int CONNECTION_LOSS_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 200L;

    public static final String OPTIMIZER_SUBPATH = "/index-optimizer";
    public static final String REGISTRY_SUBPATH = OPTIMIZER_SUBPATH + "/tasks";

    private final Supplier<ZooKeeper> zkSupplier;
    private final String optimizerRootPath;
    private final String registryRootPath;

    public OptimizerTaskRegistry(Supplier<ZooKeeper> zkSupplier, String basePath) {
        this.zkSupplier = Objects.requireNonNull(zkSupplier, "zkSupplier");
        Objects.requireNonNull(basePath, "basePath");
        this.optimizerRootPath = basePath + OPTIMIZER_SUBPATH;
        this.registryRootPath = basePath + REGISTRY_SUBPATH;
    }

    public String getRegistryRootPath() {
        return registryRootPath;
    }

    public String tablespacePath(String tablespaceUuid) {
        return registryRootPath + "/" + tablespaceUuid;
    }

    public String taskPath(String tablespaceUuid, String taskId) {
        return tablespacePath(tablespaceUuid) + "/" + taskId;
    }

    public String leasePath(String tablespaceUuid, String taskId) {
        return taskPath(tablespaceUuid, taskId) + "/lease";
    }

    public void ensureRoot() throws OptimizerTaskRegistryException {
        try {
            createIfMissing(optimizerRootPath);
            createIfMissing(registryRootPath);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to ensure task registry root " + registryRootPath, e);
        }
    }

    public void createTask(OptimizerTask task) throws OptimizerTaskRegistryException {
        Objects.requireNonNull(task, "task");
        ensureParentChain(task.getTablespaceUuid());
        String path = taskPath(task.getTablespaceUuid(), task.getTaskId());
        try {
            withConnectionLossRetry("createTask(" + task.getTaskId() + ")", () -> {
                zk().create(path, task.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return null;
            });
        } catch (KeeperException.NodeExistsException e) {
            throw new OptimizerTaskRegistryException.TaskAlreadyExists(task.getTaskId());
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to create task " + task.getTaskId(), e);
        }
    }

    public Optional<VersionedOptimizerTask> getTask(String tablespaceUuid, String taskId)
            throws OptimizerTaskRegistryException {
        return getTask(tablespaceUuid, taskId, null);
    }

    public Optional<VersionedOptimizerTask> getTask(String tablespaceUuid, String taskId, Watcher watcher)
            throws OptimizerTaskRegistryException {
        String path = taskPath(tablespaceUuid, taskId);
        Stat stat = new Stat();
        byte[] data;
        try {
            data = withConnectionLossRetry("getTask(" + taskId + ")",
                    () -> zk().getData(path, watcher, stat));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException("failed to read task " + taskId, e);
        }
        try {
            return Optional.of(new VersionedOptimizerTask(
                    OptimizerTask.deserialize(data), stat.getVersion()));
        } catch (IOException e) {
            throw new OptimizerTaskRegistryException("failed to deserialize task " + taskId, e);
        }
    }

    public List<VersionedOptimizerTask> listTasks(String tablespaceUuid)
            throws OptimizerTaskRegistryException {
        return listTasks(tablespaceUuid, null);
    }

    public List<VersionedOptimizerTask> listTasks(String tablespaceUuid, Watcher childrenWatcher)
            throws OptimizerTaskRegistryException {
        String parent = tablespacePath(tablespaceUuid);
        List<String> children;
        try {
            children = withConnectionLossRetry("listTasks(" + tablespaceUuid + ")",
                    () -> zk().getChildren(parent, childrenWatcher));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to list tasks under " + parent, e);
        }
        List<VersionedOptimizerTask> out = new ArrayList<>(children.size());
        for (String taskId : children) {
            Optional<VersionedOptimizerTask> v = getTask(tablespaceUuid, taskId);
            v.ifPresent(out::add);
        }
        return out;
    }

    public VersionedOptimizerTask casUpdateTask(VersionedOptimizerTask expected, OptimizerTask updated)
            throws OptimizerTaskRegistryException {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(updated, "updated");
        OptimizerTask previous = expected.task();
        if (!previous.getTaskId().equals(updated.getTaskId())
                || !previous.getTablespaceUuid().equals(updated.getTablespaceUuid())) {
            throw new IllegalArgumentException(
                    "cannot change addressing keys (tablespace / taskId) on a task update");
        }
        String path = taskPath(updated.getTablespaceUuid(), updated.getTaskId());
        try {
            Stat stat = withConnectionLossRetry("casUpdateTask(" + updated.getTaskId() + ")",
                    () -> zk().setData(path, updated.serialize(), expected.zkVersion()));
            return new VersionedOptimizerTask(updated, stat.getVersion());
        } catch (KeeperException.NoNodeException e) {
            throw new OptimizerTaskRegistryException.TaskNotFound(updated.getTaskId());
        } catch (KeeperException.BadVersionException e) {
            throw new OptimizerTaskRegistryException.VersionMismatch(
                    updated.getTaskId(), expected.zkVersion());
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to update task " + updated.getTaskId(), e);
        }
    }

    /**
     * Atomically deletes a task znode along with any lease child (best effort).
     * Required for the orphan-scanner "delete the task when inputs already
     * deprecated" recovery path (step 5).
     */
    public void casDeleteTask(VersionedOptimizerTask expected) throws OptimizerTaskRegistryException {
        Objects.requireNonNull(expected, "expected");
        OptimizerTask previous = expected.task();
        String taskPath = taskPath(previous.getTablespaceUuid(), previous.getTaskId());
        // Lease child must be removed first because ZK rejects delete on a node with children.
        // The lease is best-effort: it may already be gone (worker session expired) which is fine.
        try {
            withConnectionLossRetry("deleteLeaseChildFor(" + previous.getTaskId() + ")", () -> {
                try {
                    zk().delete(leasePath(previous.getTablespaceUuid(), previous.getTaskId()), -1);
                } catch (KeeperException.NoNodeException ok) {
                    // expected when no worker is mid-claim or session already expired
                }
                return null;
            });
        } catch (KeeperException | InterruptedException leaseRemoval) {
            if (leaseRemoval instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to delete lease child for task " + previous.getTaskId(), leaseRemoval);
        }
        try {
            withConnectionLossRetry("casDeleteTask(" + previous.getTaskId() + ")", () -> {
                zk().delete(taskPath, expected.zkVersion());
                return null;
            });
        } catch (KeeperException.NoNodeException e) {
            throw new OptimizerTaskRegistryException.TaskNotFound(previous.getTaskId());
        } catch (KeeperException.BadVersionException e) {
            throw new OptimizerTaskRegistryException.VersionMismatch(
                    previous.getTaskId(), expected.zkVersion());
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to delete task " + previous.getTaskId(), e);
        }
    }

    /**
     * Attempts to create the ephemeral lease znode under the task. Returns
     * {@code false} when the lease already exists (another worker is mid-claim
     * or holds the task). The caller MUST create the lease BEFORE CAS-updating
     * task state to {@code CLAIMED} so that a CAS-loser can simply delete its
     * own lease and retry — see step-6 consumer logic.
     */
    public boolean createLease(String tablespaceUuid, String taskId, String workerId)
            throws OptimizerTaskRegistryException {
        Objects.requireNonNull(workerId, "workerId");
        String path = leasePath(tablespaceUuid, taskId);
        try {
            withConnectionLossRetry("createLease(" + taskId + ")", () -> {
                zk().create(path, workerId.getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return null;
            });
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        } catch (KeeperException.NoNodeException e) {
            // Parent task znode is gone — treat as "task disappeared".
            throw new OptimizerTaskRegistryException.TaskNotFound(taskId);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException("failed to create lease for task " + taskId, e);
        }
    }

    /** Idempotent — no exception when the lease is already gone. */
    public void deleteLease(String tablespaceUuid, String taskId) throws OptimizerTaskRegistryException {
        String path = leasePath(tablespaceUuid, taskId);
        try {
            withConnectionLossRetry("deleteLease(" + taskId + ")", () -> {
                try {
                    zk().delete(path, -1);
                } catch (KeeperException.NoNodeException ok) {
                    // expected when the worker session expired before the explicit delete
                }
                return null;
            });
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException("failed to delete lease for task " + taskId, e);
        }
    }

    public boolean leaseExists(String tablespaceUuid, String taskId) throws OptimizerTaskRegistryException {
        String path = leasePath(tablespaceUuid, taskId);
        try {
            return withConnectionLossRetry("leaseExists(" + taskId + ")",
                    () -> zk().exists(path, false) != null);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException("failed to check lease for task " + taskId, e);
        }
    }

    public Optional<String> readLeaseHolder(String tablespaceUuid, String taskId)
            throws OptimizerTaskRegistryException {
        String path = leasePath(tablespaceUuid, taskId);
        try {
            return withConnectionLossRetry("readLeaseHolder(" + taskId + ")", () -> {
                byte[] data = zk().getData(path, false, null);
                return Optional.of(new String(data, StandardCharsets.UTF_8));
            });
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to read lease holder for task " + taskId, e);
        }
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private void ensureParentChain(String tablespaceUuid) throws OptimizerTaskRegistryException {
        try {
            createIfMissing(optimizerRootPath);
            createIfMissing(registryRootPath);
            createIfMissing(tablespacePath(tablespaceUuid));
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new OptimizerTaskRegistryException(
                    "failed to ensure parent path for tablespace " + tablespaceUuid, e);
        }
    }

    private void createIfMissing(String path) throws KeeperException, InterruptedException {
        try {
            zk().create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
            // idempotent
        }
    }

    private ZooKeeper zk() {
        ZooKeeper zk = zkSupplier.get();
        if (zk == null) {
            throw new IllegalStateException("ZooKeeper supplier returned null");
        }
        return zk;
    }

    @FunctionalInterface
    private interface ZkOperation<T> {
        T run() throws KeeperException, InterruptedException;
    }

    private <T> T withConnectionLossRetry(String opName, ZkOperation<T> op)
            throws KeeperException, InterruptedException {
        KeeperException.ConnectionLossException lastFailure = null;
        for (int attempt = 0; attempt < CONNECTION_LOSS_RETRIES; attempt++) {
            try {
                return op.run();
            } catch (KeeperException.ConnectionLossException e) {
                lastFailure = e;
                LOGGER.log(Level.INFO,
                        "ZK ConnectionLoss on {0} (attempt {1}/{2}); retrying",
                        new Object[]{opName, attempt + 1, CONNECTION_LOSS_RETRIES});
                Thread.sleep(RETRY_BACKOFF_MS);
            }
        }
        throw lastFailure;
    }
}
