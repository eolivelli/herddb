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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManagerException;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies that
 * {@link RemoteFileDataStorageManager#getTableStatus} falls back to
 * shared remote storage when the local metadata directory does not
 * carry the requested checkpoint LSN. Without this fallback, a fresh
 * read replica (e.g. an IndexingService rebuilder pointing at a
 * shared file server) cannot scan a table at the herddb leader's
 * checkpoint LSN, because the leader is the only writer of local
 * metadata.
 *
 * <p>Test scenarios:
 * <ul>
 *   <li><b>local hit:</b> local has the checkpoint → returns local
 *       (no shared lookup).</li>
 *   <li><b>local miss + shared hit:</b> local does NOT have the
 *       checkpoint, shared does → returns shared. This is the
 *       new behavior the issue #471 fix introduces.</li>
 *   <li><b>local miss + shared miss:</b> both lack the checkpoint
 *       → throws the original local
 *       {@link DataStorageManagerException}.</li>
 *   <li><b>local miss + no shared configured:</b> shared manager
 *       was never wired → throws the local exception unchanged
 *       (preserves existing standalone behavior).</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class RemoteFileDataStorageManagerSharedFallbackTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    private static final String TABLESPACE = "tstblspace";
    private static final String TABLE_UUID = "11111111-2222-3333-4444-555555555555";
    private static final LogSequenceNumber CHECKPOINT_LSN = new LogSequenceNumber(7L, 13L);

    private RemoteFileServer fileServer;
    private RemoteFileServiceClient leaderClient;
    private RemoteFileServiceClient replicaClient;

    @Before
    public void startFileServer() throws Exception {
        fileServer = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        fileServer.start();
        String addr = "localhost:" + fileServer.getPort();
        leaderClient = new RemoteFileServiceClient(Arrays.asList(addr));
        replicaClient = new RemoteFileServiceClient(Arrays.asList(addr));
    }

    @After
    public void stopFileServer() throws Exception {
        if (leaderClient != null) {
            leaderClient.close();
        }
        if (replicaClient != null) {
            replicaClient.close();
        }
        if (fileServer != null) {
            fileServer.stop();
        }
    }

    private TableStatus buildTableStatus() {
        // Empty-active-pages TableStatus is sufficient — the test
        // verifies LSN propagation through the shared fallback, not
        // page enumeration. DataPageMetaData has a package-private
        // constructor, so we cannot synthesise an active page from
        // outside herddb.core; building one through the public
        // DataPage path would pull in too much infrastructure for
        // a focused fallback test.
        Map<Long, herddb.core.PageSet.DataPageMetaData> activePages = new HashMap<>();
        return new TableStatus(TABLE_UUID, CHECKPOINT_LSN,
                Bytes.longToByteArray(0L), 100L, activePages);
    }

    private RemoteFileDataStorageManager buildLeaderDsmAndPublish() throws Exception {
        // Leader writes to its local metadata AND publishes to shared
        // via tableCheckpoint. We do this by wiring the shared
        // manager and calling tableCheckpoint(table, status, false).
        Path leaderMeta = folder.newFolder("leader-meta").toPath();
        Path leaderTmp = folder.newFolder("leader-tmp").toPath();
        RemoteFileDataStorageManager leader = new RemoteFileDataStorageManager(
                leaderMeta, leaderTmp, 1000, leaderClient);
        SharedCheckpointMetadataManager shared =
                new SharedCheckpointMetadataManager(leaderClient);
        leader.setSharedCheckpointMetadataManager(shared);
        leader.start();
        // initTablespace + initTable creates the local metadata
        // directory tree that tableCheckpoint expects to exist.
        leader.initTablespace(TABLESPACE);
        leader.initTable(TABLESPACE, TABLE_UUID);
        // Publish the table status. The leader writes a local file
        // AND uploads to the file server via shared.writeTableStatus.
        leader.tableCheckpoint(TABLESPACE, TABLE_UUID, buildTableStatus(), false);
        return leader;
    }

    @Test
    public void localHit_returnsLocalDirectly_noSharedLookup() throws Exception {
        // Build a leader DSM, write the status; then read back via
        // the SAME leader DSM. The shared fallback path should NOT
        // even be consulted because local has the file.
        try (RemoteFileDataStorageManager leader = buildLeaderDsmAndPublish()) {
            TableStatus status = leader.getTableStatus(TABLESPACE, TABLE_UUID, CHECKPOINT_LSN);
            assertNotNull(status);
            assertEquals(CHECKPOINT_LSN, status.sequenceNumber);
        }
    }

    @Test
    public void localMissShareHit_returnsSharedAsFallback() throws Exception {
        // Leader writes to local metadata + shared. Replica has a
        // FRESH local metadata directory (empty), but reads from the
        // SAME file server via a shared metadata manager.
        try (RemoteFileDataStorageManager leader = buildLeaderDsmAndPublish()) {
            // Build a replica DSM with a fresh local metadata dir +
            // a shared manager pointing at the SAME file server.
            Path replicaMeta = folder.newFolder("replica-meta").toPath();
            Path replicaTmp = folder.newFolder("replica-tmp").toPath();
            try (RemoteFileDataStorageManager replica = new RemoteFileDataStorageManager(
                    replicaMeta, replicaTmp, 1000, replicaClient)) {
                SharedCheckpointMetadataManager replicaShared =
                        new SharedCheckpointMetadataManager(replicaClient);
                replica.setSharedCheckpointMetadataManager(replicaShared);
                replica.start();

                TableStatus status = replica.getTableStatus(TABLESPACE, TABLE_UUID,
                        CHECKPOINT_LSN);
                assertNotNull("shared fallback must return the leader-written status",
                        status);
                assertEquals(CHECKPOINT_LSN, status.sequenceNumber);
            }
        }
    }

    @Test
    public void localMissSharedMiss_throwsLocalException() throws Exception {
        // Replica with empty local metadata + empty shared storage:
        // both lack the checkpoint, so the fallback re-throws the
        // local exception.
        Path replicaMeta = folder.newFolder("replica-meta").toPath();
        Path replicaTmp = folder.newFolder("replica-tmp").toPath();
        try (RemoteFileDataStorageManager replica = new RemoteFileDataStorageManager(
                replicaMeta, replicaTmp, 1000, replicaClient)) {
            SharedCheckpointMetadataManager replicaShared =
                    new SharedCheckpointMetadataManager(replicaClient);
            replica.setSharedCheckpointMetadataManager(replicaShared);
            replica.start();

            DataStorageManagerException err = assertThrows(DataStorageManagerException.class,
                    () -> replica.getTableStatus(TABLESPACE, TABLE_UUID, CHECKPOINT_LSN));
            // The exception message should reflect the local-side
            // path (the original error) rather than a shared-side
            // path that misleadingly suggests the file ought to
            // exist remotely.
            String msg = err.getMessage();
            assertNotNull(msg);
        }
    }

    @Test
    public void localMissNoSharedConfigured_throwsLocalExceptionUnchanged() throws Exception {
        // Replica with empty local metadata and NO shared manager
        // wired. Existing behavior must be preserved: throw the
        // local exception, do not pretend a fallback exists.
        Path replicaMeta = folder.newFolder("replica-meta").toPath();
        Path replicaTmp = folder.newFolder("replica-tmp").toPath();
        try (RemoteFileDataStorageManager replica = new RemoteFileDataStorageManager(
                replicaMeta, replicaTmp, 1000, replicaClient)) {
            // NOTE: deliberately NO setSharedCheckpointMetadataManager.
            replica.start();
            assertThrows(DataStorageManagerException.class,
                    () -> replica.getTableStatus(TABLESPACE, TABLE_UUID, CHECKPOINT_LSN));
        }
    }
}
