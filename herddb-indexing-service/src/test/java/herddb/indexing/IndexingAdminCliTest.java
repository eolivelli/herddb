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

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.indexing.admin.IndexingAdminCli;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end smoke tests for {@link IndexingAdminCli}: spin up an embedded
 * indexing service, run each sub-command via the CLI's {@code run()} entry
 * point, and assert the output on stdout/stderr.
 */
public class IndexingAdminCliTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;
    private ByteArrayOutputStream stdoutBytes;
    private ByteArrayOutputStream stderrBytes;
    private IndexingAdminCli cli;

    @Before
    public void setUp() throws Exception {
        service = new EmbeddedIndexingService(
                folder.newFolder("log").toPath(),
                folder.newFolder("data").toPath());
        service.start();

        InMemoryVectorStore store = new InMemoryVectorStore("embedding");
        for (int i = 0; i < 6; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i, -i, i * 0.25f});
        }
        service.getEngine().registerIndexForTest(
                Index.builder()
                        .name("idx_docs")
                        .table("docs")
                        .tablespace("default")
                        .type(Index.TYPE_VECTOR)
                        .column("embedding", ColumnTypes.FLOATARRAY)
                        .build(),
                store);

        stdoutBytes = new ByteArrayOutputStream();
        stderrBytes = new ByteArrayOutputStream();
        cli = new IndexingAdminCli(new PrintStream(stdoutBytes, true, StandardCharsets.UTF_8),
                new PrintStream(stderrBytes, true, StandardCharsets.UTF_8));
    }

    @After
    public void tearDown() throws Exception {
        if (service != null) {
            service.close();
        }
    }

    private String stdout() {
        return new String(stdoutBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    private String stderr() {
        return new String(stderrBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    public void testListIndexesText() {
        int rc = cli.run(new String[]{
            "list-indexes",
            "--server", service.getAddress()
        });
        assertEquals("CLI should succeed; stderr=\n" + stderr(), 0, rc);
        String out = stdout();
        assertTrue("expected table header, got:\n" + out, out.contains("TABLESPACE"));
        assertTrue(out.contains("idx_docs"));
        assertTrue(out.contains("docs"));
    }

    @Test
    public void testListIndexesJson() {
        int rc = cli.run(new String[]{
            "list-indexes",
            "--server", service.getAddress(),
            "--json"
        });
        assertEquals(0, rc);
        String out = stdout().trim();
        assertTrue("expected JSON array, got: " + out, out.startsWith("["));
        assertTrue(out.contains("\"idx_docs\""));
        assertTrue(out.contains("\"vector_count\":6"));
    }

    @Test
    public void testDescribeIndexText() {
        int rc = cli.run(new String[]{
            "describe-index",
            "--server", service.getAddress(),
            "--tablespace", "default",
            "--table", "docs",
            "--index", "idx_docs"
        });
        assertEquals(0, rc);
        String out = stdout();
        assertTrue(out.contains("Index docs.idx_docs"));
        assertTrue(out.contains("vector_count          = 6"));
        assertTrue(out.contains("store_class           = InMemoryVectorStore"));
    }

    @Test
    public void testDescribeIndexJson() {
        int rc = cli.run(new String[]{
            "describe-index",
            "--server", service.getAddress(),
            "--table", "docs",
            "--index", "idx_docs",
            "--json"
        });
        assertEquals(0, rc);
        String out = stdout().trim();
        assertTrue(out.startsWith("{"));
        assertTrue(out.contains("\"store_class\":\"InMemoryVectorStore\""));
        assertTrue(out.contains("\"similarity\":\"COSINE\""));
    }

    @Test
    public void testDescribeIndexMissing() {
        int rc = cli.run(new String[]{
            "describe-index",
            "--server", service.getAddress(),
            "--table", "ghost",
            "--index", "missing"
        });
        assertNotEquals("CLI should exit non-zero for missing index", 0, rc);
        assertTrue("stderr should mention the failure: " + stderr(),
                stderr().toLowerCase().contains("not loaded")
                        || stderr().toLowerCase().contains("error"));
    }

    @Test
    public void testStatusCommand() {
        int rc = cli.run(new String[]{
            "status",
            "--server", service.getAddress(),
            "--table", "docs",
            "--index", "idx_docs"
        });
        assertEquals(0, rc);
        String out = stdout();
        assertTrue(out.contains("vectors=6"));
        assertTrue(out.contains("status=tailing"));
    }

    @Test
    public void testListPksHexFormat() {
        int rc = cli.run(new String[]{
            "list-pks",
            "--server", service.getAddress(),
            "--table", "docs",
            "--index", "idx_docs"
        });
        assertEquals(0, rc);
        String out = stdout();
        // Expect 6 lines, each a hex-encoded 4-byte PK
        String[] lines = out.split("\n");
        assertEquals(6, lines.length);
        for (String line : lines) {
            assertTrue("Expected hex PK, got: " + line, line.matches("[0-9a-f]+"));
            assertEquals("Bytes.from_int produces 4 bytes", 8, line.length());
        }
        assertTrue(stderr().contains("# total: 6"));
    }

    @Test
    public void testListPksLimit() {
        int rc = cli.run(new String[]{
            "list-pks",
            "--server", service.getAddress(),
            "--table", "docs",
            "--index", "idx_docs",
            "--limit", "2"
        });
        assertEquals(0, rc);
        assertEquals(2, stdout().split("\n").length);
        assertTrue(stderr().contains("# total: 2"));
    }

    @Test
    public void testEngineStatsText() {
        int rc = cli.run(new String[]{
            "engine-stats",
            "--server", service.getAddress()
        });
        assertEquals(0, rc);
        String out = stdout();
        assertTrue(out.contains("Engine stats:"));
        // Issue #463: keys are left-padded to 31 chars, followed by ` = `, then
        // the value. Existing dashboards / scripts that relied on the previous
        // 28-char alignment must be updated.
        assertTrue("expected 31-char-padded loaded_index_count line, got:\n" + out,
                out.contains("loaded_index_count              = 1"));
        assertTrue("expected 31-char-padded tailer_running line, got:\n" + out,
                out.contains("tailer_running                  = true"));
        // Issue #463: the new shard-filter counter must appear in the
        // human-readable text output, alongside the rest of the metrics.
        // A freshly-started single-instance setup hasn't routed any INSERT
        // through applyInsert, so the count must be exactly 0.
        assertTrue("text output must include tailer_entries_shard_filtered, got:\n" + out,
                out.contains("tailer_entries_shard_filtered   = 0"));
    }

    /**
     * Issue #463: after refactoring {@code GetEngineStatsResponse} to a
     * generic {@code repeated MetricEntry} list, the {@code --json} output
     * must still produce a valid single JSON object containing every metric
     * key. We don't pin the order — only that every expected key is present.
     */
    @Test
    public void testEngineStatsJson() {
        int rc = cli.run(new String[]{
            "engine-stats",
            "--server", service.getAddress(),
            "--json"
        });
        assertEquals(0, rc);
        String out = stdout().trim();
        assertTrue("JSON output must be a single object, got:\n" + out,
                out.startsWith("{") && out.endsWith("}"));
        // Spot-check a few keys spanning all three value kinds the proto
        // currently uses (int64, bool, plus the new shard-filter counter).
        assertTrue("must include uptime_millis, got:\n" + out,
                out.contains("\"uptime_millis\":"));
        assertTrue("must include tailer_running, got:\n" + out,
                out.contains("\"tailer_running\":true"));
        assertTrue("must include loaded_index_count:1, got:\n" + out,
                out.contains("\"loaded_index_count\":1"));
        assertTrue("must include the new tailer_entries_shard_filtered:0, got:\n" + out,
                out.contains("\"tailer_entries_shard_filtered\":0"));
    }

    @Test
    public void testInstanceInfoJson() {
        int rc = cli.run(new String[]{
            "instance-info",
            "--server", service.getAddress(),
            "--json"
        });
        assertEquals(0, rc);
        String out = stdout().trim();
        assertTrue(out.startsWith("{"));
        assertTrue(out.contains("\"storage_type\":\"memory\""));
        assertTrue(out.contains("\"num_instances\":1"));
    }

    @Test
    public void testNoArgsPrintsUsageAndExits2() {
        int rc = cli.run(new String[]{});
        assertEquals(2, rc);
        assertTrue(stdout().contains("Usage: indexing-admin"));
    }

    @Test
    public void testUnknownCommandExits2() {
        int rc = cli.run(new String[]{"gronk"});
        assertEquals(2, rc);
        assertTrue(stderr().contains("Unknown command"));
    }

    @Test
    public void testMissingServerFlagExits2() {
        int rc = cli.run(new String[]{"list-indexes"});
        assertEquals(2, rc);
        assertTrue(stderr().contains("server"));
    }

}
