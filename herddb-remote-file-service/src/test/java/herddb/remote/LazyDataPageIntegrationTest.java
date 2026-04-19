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
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.utils.DataAccessor;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end integration test for the lazy data-page loading path: an embedded
 * {@link DBManager} uses a {@link RemoteFileDataStorageManager} that writes
 * pages in v2 format, and reopening the DB restores data by issuing byte-range
 * reads rather than downloading full pages. We assert that point lookups after
 * reopen transfer far fewer bytes than a full-page fetch would have.
 */
public class LazyDataPageIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final String TS = "ts1";

    private static class CountingClient extends RemoteFileServiceClient {
        final AtomicLong readCalls = new AtomicLong();
        final AtomicLong readBytes = new AtomicLong();

        CountingClient(List<String> servers) {
            super(servers);
        }

        @Override
        public byte[] readFileRange(String path, long offset, int length, int blockSize) {
            byte[] bytes = super.readFileRange(path, offset, length, blockSize);
            readCalls.incrementAndGet();
            if (bytes != null) {
                readBytes.addAndGet(bytes.length);
            }
            return bytes;
        }

        void reset() {
            readCalls.set(0);
            readBytes.set(0);
        }
    }

    @Test
    public void pointLookupAfterReopenUsesLazyLoad() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpPath = folder.newFolder("tmp").toPath();
        String nodeId = "localhost";

        try (RemoteFileServer server = new RemoteFileServer(
                0, folder.newFolder("remote").toPath())) {
            server.start();
            String addr = "localhost:" + server.getPort();

            // Phase 1: create table, insert a few rows with reasonably large values, checkpoint.
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client,
                                 new LazyValueCache(4L * 1024L * 1024L)),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {
                manager.start();
                manager.executeStatement(
                        new CreateTableSpaceStatement(TS, Collections.singleton(nodeId), nodeId, 1, 0, 0),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                assertTrue(manager.waitForTablespace(TS, 10000));

                Table table = Table.builder()
                        .tablespace(TS)
                        .name("t1")
                        .column("id", ColumnTypes.INTEGER)
                        .column("body", ColumnTypes.BYTEARRAY)
                        .primaryKey("id")
                        .build();
                manager.executeStatement(new CreateTableStatement(table),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);

                // Insert 50 rows with a 1 KiB body each. These should comfortably
                // fit in a single data page.
                for (int i = 0; i < 50; i++) {
                    byte[] body = new byte[1024];
                    // make bodies distinct so we can detect cross-row reads
                    for (int j = 0; j < body.length; j++) {
                        body[j] = (byte) ((i * 7 + j) & 0xff);
                    }
                    RemoteTestUtils.executeUpdate(manager,
                            "INSERT INTO " + TS + ".t1(id, body) VALUES(?, ?)",
                            Arrays.asList(i, body));
                }
                manager.checkpoint();
            }

            // Phase 2: reopen with a counting client. Issue a single-row lookup.
            // The first readPage() for this table must take the lazy path,
            // transferring only the fixed header + index section on load, and
            // a single value on the point lookup — in total << the full page.
            long transferredForOneLookup;
            try (CountingClient client = new CountingClient(Arrays.asList(addr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client,
                                 new LazyValueCache(4L * 1024L * 1024L)),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {
                manager.start();
                assertTrue(manager.waitForBootOfLocalTablespaces(10000));
                client.reset();

                try (DataScanner ds = RemoteTestUtils.scan(manager,
                        "SELECT id, body FROM " + TS + ".t1 WHERE id = ?",
                        Arrays.asList(17))) {
                    List<DataAccessor> tuples = ds.consume();
                    assertEquals("exactly one row expected", 1, tuples.size());
                    Object body = tuples.get(0).get("body");
                    assertNotNull(body);
                    byte[] bytes = (byte[]) body;
                    assertEquals(1024, bytes.length);
                    for (int j = 0; j < bytes.length; j++) {
                        assertEquals("mismatch at " + j,
                                (byte) ((17 * 7 + j) & 0xff), bytes[j]);
                    }
                }
                transferredForOneLookup = client.readBytes.get();
            }

            // A full eager read of this single page would be ~50 × 1 KiB = 50 KiB
            // plus header/index. With the lazy path we expect to see the fixed
            // header + index section + one value ≈ 22 + ~(50 × ~18 bytes) + 1 KiB
            // ≈ ~2 KiB. Give a generous upper bound in case of page-set
            // metadata reads and/or internal chatter.
            long fullPageEstimate = 50L * 1024L;
            assertTrue("point-lookup bytes transferred (" + transferredForOneLookup
                            + ") should be well below a full page (" + fullPageEstimate
                            + ")",
                    transferredForOneLookup > 0
                            && transferredForOneLookup < fullPageEstimate / 4);
        }
    }

    @Test
    public void fullScanStillWorksWithLazyPages() throws Exception {
        // A SELECT * against the whole table should still return every row
        // correctly, even though loading goes through the lazy path and each
        // record triggers a separate value fetch. This is the worst case for
        // byte-transfer efficiency, but correctness must hold.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpPath = folder.newFolder("tmp").toPath();
        String nodeId = "localhost";

        try (RemoteFileServer server = new RemoteFileServer(
                0, folder.newFolder("remote").toPath())) {
            server.start();
            String addr = "localhost:" + server.getPort();

            // Phase 1: populate.
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client,
                                 new LazyValueCache(2L * 1024L * 1024L)),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {
                manager.start();
                manager.executeStatement(
                        new CreateTableSpaceStatement(TS, Collections.singleton(nodeId), nodeId, 1, 0, 0),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                assertTrue(manager.waitForTablespace(TS, 10000));

                Table table = Table.builder()
                        .tablespace(TS)
                        .name("t1")
                        .column("id", ColumnTypes.INTEGER)
                        .column("n", ColumnTypes.STRING)
                        .primaryKey("id")
                        .build();
                manager.executeStatement(new CreateTableStatement(table),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);

                for (int i = 0; i < 100; i++) {
                    RemoteTestUtils.executeUpdate(manager,
                            "INSERT INTO " + TS + ".t1(id, n) VALUES(?, ?)",
                            Arrays.asList(i, "row-" + i));
                }
                manager.checkpoint();
            }

            // Phase 2: reopen and scan the whole table.
            try (RemoteFileServiceClient client = new RemoteFileServiceClient(Arrays.asList(addr));
                 DBManager manager = new DBManager(nodeId,
                         new FileMetadataStorageManager(metadataPath),
                         new RemoteFileDataStorageManager(dataPath, tmpPath, 1000, client,
                                 new LazyValueCache(2L * 1024L * 1024L)),
                         new FileCommitLogManager(logsPath),
                         tmpPath, null)) {
                manager.start();
                assertTrue(manager.waitForBootOfLocalTablespaces(10000));

                try (DataScanner ds = RemoteTestUtils.scan(manager,
                        "SELECT id, n FROM " + TS + ".t1",
                        Collections.emptyList())) {
                    List<DataAccessor> tuples = consume(ds);
                    assertEquals(100, tuples.size());
                    // verify a handful of rows
                    boolean[] seen = new boolean[100];
                    for (DataAccessor t : tuples) {
                        int id = ((Number) t.get("id")).intValue();
                        assertEquals("row-" + id, t.get("n").toString());
                        seen[id] = true;
                    }
                    for (int i = 0; i < 100; i++) {
                        assertTrue("row " + i + " missing", seen[i]);
                    }
                }
            }
        }
    }

    private static List<DataAccessor> consume(DataScanner ds) throws DataScannerException {
        return ds.consume();
    }
}
