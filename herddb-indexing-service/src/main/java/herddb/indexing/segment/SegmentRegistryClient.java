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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper-backed registry of segmented-v2 vector index segments.
 *
 * <p>Layout:
 * <pre>
 *   {basePath}/index-segments/                                    (PERSISTENT, root)
 *   {basePath}/index-segments/{tablespaceUuid}/                   (PERSISTENT)
 *   {basePath}/index-segments/{tablespaceUuid}/{indexUuid}/        (PERSISTENT)
 *   {basePath}/index-segments/{tablespaceUuid}/{indexUuid}/{segmentUuid}
 *       (PERSISTENT, znode data = JSON-serialized SegmentMetadata)
 * </pre>
 *
 * <p>All operations are short-lived: callers are expected to retry on
 * {@link KeeperException.ConnectionLossException} or session expiry. For CAS,
 * callers must read with {@link #getSegment} or {@link #listSegments} to get a
 * {@link VersionedSegmentMetadata} and pass it back to
 * {@link #casUpdateSegment} or {@link #casDeleteSegment}.
 *
 * <p>This class is thread-safe.
 */
public final class SegmentRegistryClient {

    /**
     * Sub-path appended to the ZK base path for the segment registry root.
     */
    public static final String REGISTRY_SUBPATH = "/index-segments";

    /**
     * Sub-path appended to the ZK base path for the per-segment swap-ack
     * subtree (issue #555). Layout:
     * <pre>
     *   {basePath}/index-segments-acks/                  (PERSISTENT, root)
     *   {basePath}/index-segments-acks/{segmentUuid}/    (PERSISTENT, created when PROVISIONAL is staged)
     *   {basePath}/index-segments-acks/{segmentUuid}/{serviceId}  (EPHEMERAL, created by each interested IS pod after adoptExternalSegment)
     * </pre>
     * Stored OUTSIDE the segment znode tree on purpose: {@link #casDeleteSegment}
     * requires the segment znode to be childless, and a per-segment acks
     * subtree under the segment would block deletion at retention time.
     */
    public static final String ACKS_SUBPATH = "/index-segments-acks";

    /**
     * Number of attempts for a ZK operation that fails with
     * {@link KeeperException.ConnectionLossException} (review item D4).
     * Bounded — each retry waits {@link #RETRY_BACKOFF_MS} so we never busy-loop.
     * After exhausting all attempts the original exception bubbles up wrapped
     * in a {@link SegmentRegistryException}.
     */
    static final int CONNECTION_LOSS_RETRIES = 5;
    static final long RETRY_BACKOFF_MS = 200L;

    private final Supplier<ZooKeeper> zkSupplier;
    private final String registryRootPath;
    private final String acksRootPath;

    /**
     * @param zkSupplier returns a live, connected {@link ZooKeeper} instance. The
     *                  supplier is called on every operation so the caller can swap the
     *                  underlying ZK client across session expiry.
     * @param basePath  HerdDB's base ZK path (e.g. {@code /herd}). The registry stores
     *                  its data under {@code basePath + "/index-segments"}.
     */
    public SegmentRegistryClient(Supplier<ZooKeeper> zkSupplier, String basePath) {
        this.zkSupplier = Objects.requireNonNull(zkSupplier, "zkSupplier");
        Objects.requireNonNull(basePath, "basePath");
        this.registryRootPath = basePath + REGISTRY_SUBPATH;
        this.acksRootPath = basePath + ACKS_SUBPATH;
    }

    /**
     * Returns the absolute ZK path of the registry root.
     */
    public String getRegistryRootPath() {
        return registryRootPath;
    }

    /**
     * Returns the absolute ZK path of a segment znode.
     */
    public String segmentPath(String tablespaceUuid, String indexUuid, String segmentUuid) {
        return registryRootPath + "/" + tablespaceUuid + "/" + indexUuid + "/" + segmentUuid;
    }

    /**
     * Returns the absolute ZK path of an index znode (parent of all segments for that index).
     */
    public String indexPath(String tablespaceUuid, String indexUuid) {
        return registryRootPath + "/" + tablespaceUuid + "/" + indexUuid;
    }

    /**
     * Returns the absolute ZK path of a tablespace znode (parent of all indexes for that tablespace).
     */
    public String tablespacePath(String tablespaceUuid) {
        return registryRootPath + "/" + tablespaceUuid;
    }

    /**
     * Returns the absolute ZK path of the acks subtree root.
     */
    public String getAcksRootPath() {
        return acksRootPath;
    }

    /**
     * Returns the absolute ZK path of the per-segment acks parent znode
     * (PERSISTENT, holds ephemeral child znodes from each interested IS pod).
     */
    public String acksParentPath(String segmentUuid) {
        return acksRootPath + "/" + segmentUuid;
    }

    /**
     * Returns the absolute ZK path of an ack znode owned by a specific IS pod
     * (EPHEMERAL, dies with the IS pod's ZK session).
     */
    public String ackPath(String segmentUuid, String serviceId) {
        return acksParentPath(segmentUuid) + "/" + serviceId;
    }

    /**
     * Creates the registry roots if they do not exist. Idempotent. Call once at
     * startup (the IS already does this for its own metadata; the optimizer will too).
     */
    public void ensureRoot() throws SegmentRegistryException {
        try {
            createIfMissing(registryRootPath);
            createIfMissing(acksRootPath);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException(
                    "failed to ensure registry roots " + registryRootPath
                            + " / " + acksRootPath, e);
        }
    }

    /**
     * Creates a new segment znode. Lazily creates parent znodes (tablespace, index)
     * if missing.
     *
     * @throws SegmentRegistryException.SegmentAlreadyExists if a segment with the same
     *         UUID already exists for this index.
     */
    public void createSegment(SegmentMetadata segment) throws SegmentRegistryException {
        Objects.requireNonNull(segment, "segment");
        ensureParentChain(segment.getTablespaceUuid(), segment.getIndexUuid());
        String path = segmentPath(segment.getTablespaceUuid(), segment.getIndexUuid(), segment.getSegmentUuid());
        try {
            withConnectionLossRetry("createSegment(" + segment.getSegmentUuid() + ")", () -> {
                zk().create(path, segment.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return null;
            });
        } catch (KeeperException.NodeExistsException e) {
            throw new SegmentRegistryException.SegmentAlreadyExists(segment.getSegmentUuid());
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to create segment " + segment.getSegmentUuid(), e);
        }
    }

    /**
     * Reads a segment, returning {@link Optional#empty()} if it does not exist.
     */
    public Optional<VersionedSegmentMetadata> getSegment(String tablespaceUuid, String indexUuid, String segmentUuid)
            throws SegmentRegistryException {
        return getSegment(tablespaceUuid, indexUuid, segmentUuid, null);
    }

    /**
     * Reads a segment and arms the supplied watcher for znode-data changes.
     */
    public Optional<VersionedSegmentMetadata> getSegment(String tablespaceUuid, String indexUuid, String segmentUuid,
            Watcher watcher) throws SegmentRegistryException {
        String path = segmentPath(tablespaceUuid, indexUuid, segmentUuid);
        try {
            return withConnectionLossRetry("getSegment(" + segmentUuid + ")", () -> {
                Stat stat = new Stat();
                byte[] data = zk().getData(path, watcher, stat);
                return Optional.of(new VersionedSegmentMetadata(
                        SegmentMetadata.deserialize(data), stat.getVersion()));
            });
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to read segment " + segmentUuid, e);
        }
    }

    /**
     * Lists all segments belonging to an index. Returns an empty list if the index
     * has no segments registered (or has not yet been created).
     */
    public List<VersionedSegmentMetadata> listSegments(String tablespaceUuid, String indexUuid)
            throws SegmentRegistryException {
        return listSegments(tablespaceUuid, indexUuid, null);
    }

    /**
     * Lists all segments belonging to an index and arms the supplied watcher for
     * children changes on the index znode. The watcher fires on add/remove of segments.
     */
    public List<VersionedSegmentMetadata> listSegments(String tablespaceUuid, String indexUuid,
            Watcher childrenWatcher) throws SegmentRegistryException {
        String parent = indexPath(tablespaceUuid, indexUuid);
        List<String> children;
        try {
            children = withConnectionLossRetry("listSegments(" + indexUuid + ")",
                    () -> zk().getChildren(parent, childrenWatcher));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to list segments under " + parent, e);
        }
        List<VersionedSegmentMetadata> out = new ArrayList<>(children.size());
        for (String segmentUuid : children) {
            Optional<VersionedSegmentMetadata> v = getSegment(tablespaceUuid, indexUuid, segmentUuid);
            v.ifPresent(out::add);
        }
        return out;
    }

    /**
     * Lists indexes (UUIDs) registered under a tablespace. Returns empty list if the
     * tablespace path does not exist.
     */
    public List<String> listIndexes(String tablespaceUuid) throws SegmentRegistryException {
        try {
            return withConnectionLossRetry("listIndexes(" + tablespaceUuid + ")",
                    () -> zk().getChildren(tablespacePath(tablespaceUuid), false));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to list indexes under " + tablespacePath(tablespaceUuid), e);
        }
    }

    /**
     * Lists tablespaces (UUIDs) registered under the registry root. Returns empty list
     * if no tablespace has any segment yet.
     */
    public List<String> listTablespaces() throws SegmentRegistryException {
        try {
            return withConnectionLossRetry("listTablespaces",
                    () -> zk().getChildren(registryRootPath, false));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to list tablespaces under " + registryRootPath, e);
        }
    }

    /**
     * Atomically replaces a segment's metadata. The {@code expected.zkVersion()} must
     * match the current znode version, otherwise {@link SegmentRegistryException.VersionMismatch}
     * is thrown.
     *
     * @param expected the previously-read metadata + version. The new metadata must have the
     *                 same {@code (tablespaceUuid, indexUuid, segmentUuid)} as the expected one
     *                 (these are the addressing keys; mutating them is not supported).
     * @param updated  the new metadata to write.
     * @return the new {@link VersionedSegmentMetadata} carrying the updated zkVersion.
     */
    public VersionedSegmentMetadata casUpdateSegment(VersionedSegmentMetadata expected, SegmentMetadata updated)
            throws SegmentRegistryException {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(updated, "updated");
        SegmentMetadata previous = expected.metadata();
        if (!previous.getSegmentUuid().equals(updated.getSegmentUuid())
                || !previous.getIndexUuid().equals(updated.getIndexUuid())
                || !previous.getTablespaceUuid().equals(updated.getTablespaceUuid())) {
            throw new IllegalArgumentException(
                    "cannot change addressing keys (tablespace/index/segment uuid) on a segment update");
        }
        String path = segmentPath(updated.getTablespaceUuid(), updated.getIndexUuid(), updated.getSegmentUuid());
        try {
            Stat stat = withConnectionLossRetry("casUpdateSegment(" + updated.getSegmentUuid() + ")",
                    () -> zk().setData(path, updated.serialize(), expected.zkVersion()));
            return new VersionedSegmentMetadata(updated, stat.getVersion());
        } catch (KeeperException.NoNodeException e) {
            throw new SegmentRegistryException.SegmentNotFound(updated.getSegmentUuid());
        } catch (KeeperException.BadVersionException e) {
            throw new SegmentRegistryException.VersionMismatch(updated.getSegmentUuid(), expected.zkVersion());
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to update segment " + updated.getSegmentUuid(), e);
        }
    }

    /**
     * Atomically deletes a segment znode. The {@code expected.zkVersion()} must
     * match.
     */
    public void casDeleteSegment(VersionedSegmentMetadata expected) throws SegmentRegistryException {
        Objects.requireNonNull(expected, "expected");
        SegmentMetadata previous = expected.metadata();
        String path = segmentPath(previous.getTablespaceUuid(), previous.getIndexUuid(), previous.getSegmentUuid());
        try {
            withConnectionLossRetry("casDeleteSegment(" + previous.getSegmentUuid() + ")", () -> {
                zk().delete(path, expected.zkVersion());
                return null;
            });
        } catch (KeeperException.NoNodeException e) {
            throw new SegmentRegistryException.SegmentNotFound(previous.getSegmentUuid());
        } catch (KeeperException.BadVersionException e) {
            throw new SegmentRegistryException.VersionMismatch(previous.getSegmentUuid(), expected.zkVersion());
        } catch (KeeperException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to delete segment " + previous.getSegmentUuid(), e);
        }
    }

    /**
     * Atomically swaps a PROVISIONAL output to ACTIVE and every input from
     * ACTIVE to DEPRECATED (with {@code replacedBy=[output.uuid]} and
     * {@code retentionUntilEpochMillis}) inside a single {@code ZooKeeper.multi(...)}
     * transaction. This is the issue #555 fix: the swap is observable as
     * either "before" or "after" by every watcher snapshot — never a partial
     * "output ACTIVE without inputs DEPRECATED" or "inputs DEPRECATED without
     * output ACTIVE" intermediate state.
     *
     * <p>Behaviour:
     * <ul>
     *   <li>{@code provisional.metadata().getState()} must be
     *       {@link SegmentState#PROVISIONAL}. The committed output carries the
     *       same metadata with {@code state=ACTIVE}.</li>
     *   <li>Every input must be in {@link SegmentState#ACTIVE}; the new state
     *       carries {@code replacedBy=[output.uuid]} +
     *       {@code retentionUntilEpochMillis}.</li>
     *   <li>On {@code BadVersionException} or {@code NoNodeException} from any
     *       element, the entire multi-op rolls back and the caller receives a
     *       {@link SegmentRegistryException.VersionMismatch} (we surface the
     *       offending UUID so the caller can re-read and decide whether to
     *       retry or abort).</li>
     * </ul>
     *
     * <p>Hot-path note: {@code ZooKeeper.multi} is one round-trip with one
     * server-side transaction, so the cost is comparable to a single CAS no
     * matter how many inputs are deprecated together.
     *
     * @return the committed output as a {@link VersionedSegmentMetadata} (the
     *     znode version is bumped by ZK from {@code provisional.zkVersion()};
     *     we read it back via {@link #getSegment} because {@code multi} does
     *     not return per-op stats for {@code setData}).
     */
    public VersionedSegmentMetadata atomicSwap(VersionedSegmentMetadata provisional,
                                               List<VersionedSegmentMetadata> inputsToDeprecate,
                                               long retentionUntilEpochMillis)
            throws SegmentRegistryException {
        Objects.requireNonNull(provisional, "provisional");
        Objects.requireNonNull(inputsToDeprecate, "inputsToDeprecate");
        SegmentMetadata provMeta = provisional.metadata();
        if (provMeta.getState() != SegmentState.PROVISIONAL) {
            throw new IllegalStateException(
                    "atomicSwap: expected output " + provMeta.getSegmentUuid()
                            + " to be PROVISIONAL, was " + provMeta.getState());
        }
        SegmentMetadata committed = provMeta.toBuilder()
                .state(SegmentState.ACTIVE)
                .build();
        String committedPath = segmentPath(committed.getTablespaceUuid(),
                committed.getIndexUuid(), committed.getSegmentUuid());

        List<Op> ops = new ArrayList<>(1 + inputsToDeprecate.size());
        ops.add(Op.setData(committedPath, committed.serialize(), provisional.zkVersion()));
        for (VersionedSegmentMetadata in : inputsToDeprecate) {
            SegmentMetadata inMeta = in.metadata();
            if (inMeta.getState() != SegmentState.ACTIVE) {
                throw new IllegalStateException(
                        "atomicSwap: expected input " + inMeta.getSegmentUuid()
                                + " to be ACTIVE, was " + inMeta.getState());
            }
            SegmentMetadata deprecated = inMeta.toBuilder()
                    .state(SegmentState.DEPRECATED)
                    .replacedBy(Collections.singletonList(committed.getSegmentUuid()))
                    .retentionUntilEpochMillis(retentionUntilEpochMillis)
                    .build();
            String inPath = segmentPath(inMeta.getTablespaceUuid(),
                    inMeta.getIndexUuid(), inMeta.getSegmentUuid());
            ops.add(Op.setData(inPath, deprecated.serialize(), in.zkVersion()));
        }

        try {
            withConnectionLossRetry("atomicSwap(" + committed.getSegmentUuid() + ")",
                    (ZkOperation<Void>) () -> {
                        zk().multi(ops);
                        return null;
                    });
        } catch (KeeperException.BadVersionException badVersion) {
            throw new SegmentRegistryException.VersionMismatch(
                    committed.getSegmentUuid(), provisional.zkVersion());
        } catch (KeeperException.NoNodeException missing) {
            throw new SegmentRegistryException.SegmentNotFound(committed.getSegmentUuid());
        } catch (KeeperException e) {
            // multi() collapses per-op failures into the first one's exception type.
            // Other KeeperException subclasses (e.g. session expired, unsupported)
            // bubble up here; surface as a generic registry failure.
            throw new SegmentRegistryException(
                    "atomicSwap(" + committed.getSegmentUuid() + ") failed", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SegmentRegistryException(
                    "atomicSwap(" + committed.getSegmentUuid() + ") interrupted", ie);
        } catch (IOException io) {
            // Comes from withConnectionLossRetry signature; not actually thrown by the lambda.
            throw new SegmentRegistryException(
                    "atomicSwap(" + committed.getSegmentUuid() + ") I/O", io);
        }

        // Re-read the committed znode to capture the new zkVersion bumped by setData.
        Optional<VersionedSegmentMetadata> reread = getSegment(
                committed.getTablespaceUuid(), committed.getIndexUuid(),
                committed.getSegmentUuid());
        return reread.orElseThrow(() -> new SegmentRegistryException(
                "atomicSwap succeeded but committed znode for "
                        + committed.getSegmentUuid() + " was deleted out from under us"));
    }

    /**
     * Best-effort no-op-if-already-present create of the persistent parent
     * for ack ephemeral children (issue #555). The optimizer calls this
     * right after staging a {@code PROVISIONAL} output so that IS pods can
     * subsequently create ephemeral children even if the optimizer pod that
     * staged it dies before the first ack arrives.
     */
    public void createSwapAckParent(String segmentUuid) throws SegmentRegistryException {
        Objects.requireNonNull(segmentUuid, "segmentUuid");
        try {
            createIfMissing(acksRootPath);
            withConnectionLossRetry("createSwapAckParent(" + segmentUuid + ")", () -> {
                try {
                    zk().create(acksParentPath(segmentUuid), new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException ok) {
                    // idempotent — orphan recovery may re-stage onto an existing parent
                }
                return null;
            });
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException(
                    "failed to create swap-ack parent for " + segmentUuid, e);
        }
    }

    /**
     * Creates the ephemeral ack znode for the given (segmentUuid, serviceId)
     * pair. Called by each interested IS pod after a successful
     * {@code adoptExternalSegment} (issue #555). Idempotent on
     * {@link KeeperException.NodeExistsException} so a watcher re-fire does
     * not raise.
     *
     * <p>The znode payload is the {@code serviceId} as UTF-8 bytes so operators
     * inspecting ZK can see which pod made the ack.
     */
    public void createSwapAckNode(String segmentUuid, String serviceId)
            throws SegmentRegistryException {
        Objects.requireNonNull(segmentUuid, "segmentUuid");
        Objects.requireNonNull(serviceId, "serviceId");
        String path = ackPath(segmentUuid, serviceId);
        try {
            withConnectionLossRetry("createSwapAckNode(" + segmentUuid + "/" + serviceId + ")", () -> {
                try {
                    zk().create(path, serviceId.getBytes(StandardCharsets.UTF_8),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (KeeperException.NodeExistsException ok) {
                    // Another adoption (or watcher refresh on the same pod) already
                    // wrote the ack. Idempotent: the ack semantically asserts "this
                    // pod has the segment loaded", so multiple writes converge.
                }
                return null;
            });
        } catch (KeeperException.NoNodeException missingParent) {
            // The parent acks subtree does not exist. Three benign cases:
            //  (1) the swap-completion path already tore it down (multi-op
            //      committed), (2) the abort path tore it down, or (3) the
            //      optimizer pod crashed between createSegment(PROVISIONAL)
            //      and createSwapAckParent so the parent was never created.
            // In all three the IS pod's ack is not (or no longer) needed.
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException(
                    "failed to create swap-ack node for "
                            + segmentUuid + "/" + serviceId, e);
        }
    }

    /**
     * Lists the {@code serviceId}s that currently have an ephemeral ack znode
     * under the per-segment acks parent. Returns an empty list when the parent
     * does not exist (e.g. acks already torn down, or never staged). Used by
     * the consumer to decide whether to fire the multi-op or keep waiting.
     */
    public List<String> listSwapAcks(String segmentUuid, Watcher childrenWatcher)
            throws SegmentRegistryException {
        Objects.requireNonNull(segmentUuid, "segmentUuid");
        String parent = acksParentPath(segmentUuid);
        try {
            return withConnectionLossRetry("listSwapAcks(" + segmentUuid + ")",
                    () -> zk().getChildren(parent, childrenWatcher));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException(
                    "failed to list swap-acks for " + segmentUuid, e);
        }
    }

    /**
     * Maximum re-list-and-retry passes {@link #deleteSwapAcksTree} performs
     * when a late ephemeral ack lands between {@code getChildren} and the
     * parent {@code delete} (raising {@code NotEmptyException}). Bounded so a
     * pathologically chatty IS pod cannot loop the optimizer forever — after
     * the bound the parent znode is left for the next caller / orphan sweep.
     */
    private static final int DELETE_ACKS_TREE_MAX_PASSES = 4;

    /**
     * Recursively deletes the per-segment acks subtree. Called on multi-op
     * success (the swap is committed; acks are no longer needed) and on the
     * abort path. Idempotent: a missing subtree is a no-op.
     *
     * <p>If a late ephemeral ack lands between the {@code getChildren} read
     * and the parent {@code delete}, the parent delete raises
     * {@code NotEmptyException}; the method re-lists the children and retries,
     * bounded by {@link #DELETE_ACKS_TREE_MAX_PASSES}. After the bound the
     * parent znode is left in place — it is harmless (the segment is already
     * committed or aborted, so the acks subtree is never read again) and the
     * orphan-PROVISIONAL sweep / a future call reclaims it.
     */
    public void deleteSwapAcksTree(String segmentUuid) throws SegmentRegistryException {
        Objects.requireNonNull(segmentUuid, "segmentUuid");
        String parent = acksParentPath(segmentUuid);
        try {
            withConnectionLossRetry("deleteSwapAcksTree(" + segmentUuid + ")", () -> {
                for (int pass = 0; pass < DELETE_ACKS_TREE_MAX_PASSES; pass++) {
                    List<String> children;
                    try {
                        children = zk().getChildren(parent, false);
                    } catch (KeeperException.NoNodeException gone) {
                        return null;
                    }
                    for (String c : children) {
                        try {
                            zk().delete(parent + "/" + c, -1);
                        } catch (KeeperException.NoNodeException ok) {
                            // ephemeral races: the IS pod's session just expired
                        }
                    }
                    try {
                        zk().delete(parent, -1);
                        return null;
                    } catch (KeeperException.NoNodeException ok) {
                        return null;
                    } catch (KeeperException.NotEmptyException raceWithLateAck) {
                        // A new ephemeral ack landed between getChildren and
                        // delete. Re-list and retry up to the pass bound.
                        java.util.logging.Logger.getLogger(SegmentRegistryClient.class.getName())
                                .log(java.util.logging.Level.FINE,
                                        "deleteSwapAcksTree({0}): late ack landed (pass {1}); re-listing",
                                        new Object[]{segmentUuid, pass + 1});
                    }
                }
                // Bound exhausted: leave the (harmless) parent znode behind.
                java.util.logging.Logger.getLogger(SegmentRegistryClient.class.getName())
                        .log(java.util.logging.Level.FINE,
                                "deleteSwapAcksTree({0}): acks parent still non-empty after {1}"
                                        + " passes; leaving it for a later sweep",
                                new Object[]{segmentUuid, DELETE_ACKS_TREE_MAX_PASSES});
                return null;
            });
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException(
                    "failed to delete swap-acks tree for " + segmentUuid, e);
        }
    }

    private void ensureParentChain(String tablespaceUuid, String indexUuid) throws SegmentRegistryException {
        try {
            createIfMissing(registryRootPath);
            createIfMissing(tablespacePath(tablespaceUuid));
            createIfMissing(indexPath(tablespaceUuid, indexUuid));
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to ensure parent chain for "
                    + tablespaceUuid + "/" + indexUuid, e);
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

    /**
     * Functional interface for a ZK operation that may throw KeeperException or
     * InterruptedException — used by {@link #withConnectionLossRetry}.
     */
    @FunctionalInterface
    interface ZkOperation<T> {
        T run() throws KeeperException, InterruptedException, java.io.IOException;
    }

    /**
     * Retries the supplied operation on transient {@link KeeperException.ConnectionLossException}
     * (review item D4). Bounded by {@link #CONNECTION_LOSS_RETRIES} with a
     * {@link #RETRY_BACKOFF_MS} sleep between attempts. Other ZK errors and the
     * checked {@code KeeperException} subclasses propagate immediately so callers
     * (e.g. CAS callers) can map them to typed
     * {@link SegmentRegistryException} variants.
     *
     * <p>Hot-path note: the retry path adds at most {@code retries × backoff} of
     * latency on a degraded ZK; on the happy path it has zero overhead beyond a
     * direct lambda invocation.
     */
    private <T> T withConnectionLossRetry(String opName, ZkOperation<T> op)
            throws KeeperException, InterruptedException, java.io.IOException {
        KeeperException.ConnectionLossException lastFailure = null;
        for (int attempt = 0; attempt < CONNECTION_LOSS_RETRIES; attempt++) {
            try {
                return op.run();
            } catch (KeeperException.ConnectionLossException e) {
                lastFailure = e;
                java.util.logging.Logger.getLogger(SegmentRegistryClient.class.getName())
                        .log(java.util.logging.Level.INFO,
                                "ZK ConnectionLoss on {0} (attempt {1}/{2}); retrying",
                                new Object[]{opName, attempt + 1, CONNECTION_LOSS_RETRIES});
                Thread.sleep(RETRY_BACKOFF_MS);
            }
        }
        throw lastFailure;
    }
}
