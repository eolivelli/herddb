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

package herddb.sql;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage of the {@code CREATE VECTOR INDEX ... WITH ...} parsing
 * pipeline (issue #451 regression gate).
 *
 * <p>The {@code numShards} property in the WITH clause drives cross-instance
 * routing in {@code IndexingServiceEngine.isAcceptedLocally}: if the SQL
 * parser silently drops the property, every indexing-service replica
 * short-circuits to "accept everything" and re-indexes every vector — which
 * is exactly what issue #451 observed in production.
 *
 * <p>Pre-fix, {@code JSQLParserPlanner.extractIndexWithClause} silently
 * skipped tokens without an {@code =}, so the bench DDL
 * {@code ... fusedPQ=true numShards 4} (with a space) created an Index with
 * an empty properties map. The fix tightens the parser so missing-{@code =}
 * tokens throw {@link StatementExecutionException} loudly. These tests lock
 * down both the success path (key=value) and the failure path (the silent-
 * drop regression).
 */
public class CreateVectorIndexWithClauseTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmoDir)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        // The default planner rejects CREATE VECTOR INDEX outright;
        // JSQLParserPlanner is the only one that pre-processes the WITH
        // clause via extractIndexWithClause.
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    private void bootstrapTablespaceAndTable(DBManager manager) throws Exception {
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager,
                "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                Collections.emptyList());
    }

    private Index getVectorIndex(DBManager manager) {
        return manager.getTableSpaceManager("tblspace1")
                .getIndexesOnTable("t1").get("vidx").getIndex();
    }

    /**
     * The full WITH clause (every property in {@code key=value} form) must
     * survive the parser end-to-end into {@link Index#properties}. This is
     * the shape that {@code VectorBench.buildCreateVectorIndexSql} now
     * emits after the issue #451 fix.
     */
    @Test
    public void numShardsKeyEqualsValueLandsOnIndex() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=cosine"
                            + " fusedPQ=true numShards=4",
                    Collections.emptyList());

            Index idx = getVectorIndex(manager);
            assertNotNull("vector index must exist", idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            assertEquals("16", idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("100", idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            // Issue #521: similarity is now normalized to UPPERCASE at DDL time.
            assertEquals("COSINE", idx.properties.get("similarity"));
            assertEquals("true", idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));
            // The headline assertion: the routing knob actually made it
            // through. Pre-fix this returned null and the cluster
            // short-circuited to "accept everything".
            assertEquals("4", idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }

    /**
     * Comma-separated key=value pairs work too — {@code split("[\\s,]+")}
     * tolerates both whitespace and commas as token separators, and that
     * shape is the documented form in the planner JavaDoc.
     */
    @Test
    public void commaSeparatedKeyEqualsValueLandsOnIndex() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=32, beamWidth=200, numShards=8",
                    Collections.emptyList());

            Index idx = getVectorIndex(manager);
            assertEquals("32", idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("200", idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            assertEquals("8", idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }

    /**
     * Issue #451 regression: pre-fix, {@code numShards 4} (with a space, no
     * {@code =}) was silently dropped, the Index was persisted with no
     * {@code numShards} property, and every IS replica re-indexed every
     * vector. After the fix, the parser refuses the malformed token loudly
     * so the misconfiguration cannot ship to production.
     */
    @Test
    public void numShardsWithSpaceSyntaxIsRejected() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                                + " WITH m=16 beamWidth=100 similarity=cosine"
                                + " fusedPQ=true numShards 4",
                        Collections.emptyList());
                fail("expected the planner to reject space-separated numShards 4");
            } catch (StatementExecutionException ex) {
                // The error must mention the offending token AND the
                // expected key=value shape so an operator can fix it
                // without spelunking through the parser source.
                String msg = ex.getMessage() == null ? "" : ex.getMessage();
                assertTrue("error must mention 'numShards' or '4', got: " + msg,
                        msg.contains("numShards") || msg.contains("'4'"));
                assertTrue("error must mention the expected key=value shape, got: " + msg,
                        msg.contains("key=value"));
            }
        }
    }

    /**
     * Bare keyword tokens (no value at all) must also be refused, not silently
     * dropped — this is the same class of bug as issue #451 with a slightly
     * different shape.
     */
    @Test
    public void bareKeywordWithoutValueIsRejected() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                                + " WITH m=16 fusedPQ",
                        Collections.emptyList());
                fail("expected the planner to reject the bare 'fusedPQ' token");
            } catch (StatementExecutionException ex) {
                String msg = ex.getMessage() == null ? "" : ex.getMessage();
                assertTrue("error must mention 'fusedPQ', got: " + msg,
                        msg.contains("fusedPQ"));
            }
        }
    }

    /**
     * Locks the literal DDL shape that {@code VectorBench.buildCreateVectorIndexSql}
     * emits in production against the parser, so a refactor on either side
     * cannot silently regress. The string is intentionally hard-coded
     * (rather than imported from {@code VectorBench}) to avoid a circular
     * herddb-core ↔ vector-testings dependency — the matching producer-side
     * assertion lives in
     * {@code VectorBenchTest.buildCreateVectorIndexSqlDefaultIncludesNumShardsFour}.
     * If that test changes, this one must change in lockstep.
     */
    @Test
    public void vectorBenchProductionDdlParsesIntoExpectedIndex() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            // Mirror VectorBench.buildCreateVectorIndexSql output verbatim.
            // Anything that drifts here (e.g. dropping the '=' on numShards)
            // breaks the indexing-service cluster's load distribution — see
            // issue #451.
            // Issue #520: VectorBench now always emits neighborOverflow and alpha.
            // Issue #521: similarity is stored as UPPERCASE after normalization.
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=euclidean"
                            + " fusedPQ=true neighborOverflow=1.2 alpha=1.4 numShards=4",
                    Collections.emptyList());

            Index idx = getVectorIndex(manager);
            assertEquals("16",        idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("100",       idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            // Issue #521: similarity is normalized to UPPERCASE at DDL time.
            assertEquals("EUCLIDEAN", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
            assertEquals("true",      idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));
            assertEquals("1.2",       idx.properties.get(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW));
            assertEquals("1.4",       idx.properties.get(VectorIndexManager.PROP_ALPHA));
            assertEquals("4",         idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
            // The whole point of the fix: numShards must round-trip with
            // exactly the value the user typed, so the IS routing math
            // (shardId % numInstances == instanceId) actually splits the
            // workload across replicas.
            assertEquals("Index must carry exactly the 7 WITH-clause props",
                    7, idx.properties.size());
        }
    }

    /**
     * Trailing-{@code =} tokens (key with no value) must be refused too — the
     * resulting empty-string value would never round-trip through any
     * downstream property reader, so failing fast is the right call.
     */
    @Test
    public void keyWithEmptyValueIsRejected() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                                + " WITH m=16 numShards=",
                        Collections.emptyList());
                fail("expected the planner to reject the empty-value 'numShards=' token");
            } catch (StatementExecutionException ex) {
                String msg = ex.getMessage() == null ? "" : ex.getMessage();
                assertTrue("error must mention 'numShards', got: " + msg,
                        msg.contains("numShards"));
            }
        }
    }
}
