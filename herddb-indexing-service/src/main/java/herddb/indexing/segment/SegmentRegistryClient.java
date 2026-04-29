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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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

    private final Supplier<ZooKeeper> zkSupplier;
    private final String registryRootPath;

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
     * Creates the registry root znode if it does not exist. Idempotent. Call once at
     * startup (the IS already does this for its own metadata; the optimizer will too).
     */
    public void ensureRoot() throws SegmentRegistryException {
        try {
            createIfMissing(registryRootPath);
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to ensure registry root " + registryRootPath, e);
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
            zk().create(path, segment.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            throw new SegmentRegistryException.SegmentAlreadyExists(segment.getSegmentUuid());
        } catch (KeeperException | InterruptedException e) {
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
            Stat stat = new Stat();
            byte[] data = zk().getData(path, watcher, stat);
            return Optional.of(new VersionedSegmentMetadata(SegmentMetadata.deserialize(data), stat.getVersion()));
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
            children = zk().getChildren(parent, childrenWatcher);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException e) {
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
            return zk().getChildren(tablespacePath(tablespaceUuid), false);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException e) {
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
            return zk().getChildren(registryRootPath, false);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (KeeperException | InterruptedException e) {
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
            Stat stat = zk().setData(path, updated.serialize(), expected.zkVersion());
            return new VersionedSegmentMetadata(updated, stat.getVersion());
        } catch (KeeperException.NoNodeException e) {
            throw new SegmentRegistryException.SegmentNotFound(updated.getSegmentUuid());
        } catch (KeeperException.BadVersionException e) {
            throw new SegmentRegistryException.VersionMismatch(updated.getSegmentUuid(), expected.zkVersion());
        } catch (KeeperException | InterruptedException e) {
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
            zk().delete(path, expected.zkVersion());
        } catch (KeeperException.NoNodeException e) {
            throw new SegmentRegistryException.SegmentNotFound(previous.getSegmentUuid());
        } catch (KeeperException.BadVersionException e) {
            throw new SegmentRegistryException.VersionMismatch(previous.getSegmentUuid(), expected.zkVersion());
        } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SegmentRegistryException("failed to delete segment " + previous.getSegmentUuid(), e);
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
}
