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

package herddb.core.indexes;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for CREATE VECTOR INDEX WITH-clause normalization and default-materialization
 * (issues #520 and #521).
 *
 * <p>Exercises:
 * <ul>
 *   <li>lowercase {@code similarity} is normalized to UPPERCASE in stored properties.</li>
 *   <li>unknown similarity values are rejected at DDL time.</li>
 *   <li>absent jvector parameters are filled with canonical defaults so every downstream
 *       consumer (optimizer, replicas) always sees a complete property set.</li>
 *   <li>user-supplied values take precedence over the defaults.</li>
 * </ul>
 */
public class VectorIndexWithClauseTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path dataPath, Path logsPath,
                                   Path metadataPath, Path tmoDir) throws Exception {
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    /** Helper: creates a fresh tablespace + table ready for index creation. */
    private DBManager startManagerWithTable() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir);
        manager.start();
        CreateTableSpaceStatement st = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                Collections.emptyList());
        return manager;
    }

    // -----------------------------------------------------------------------
    // Issue #521 — similarity normalization
    // -----------------------------------------------------------------------

    /**
     * {@code similarity=euclidean} (lowercase) in the WITH clause must be stored as
     * {@code EUCLIDEAN} in the index properties.  This is the exact form required by
     * {@code VectorSimilarityFunction.valueOf()} and therefore by the optimizer.
     */
    @Test
    public void testLowercaseSimilarityIsNormalized() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=euclidean",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("EUCLIDEAN", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
        }
    }

    /**
     * {@code similarity=COSINE} (already uppercase) must also be stored as-is.
     */
    @Test
    public void testUppercaseSimilarityIsPreserved() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=COSINE",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("COSINE", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
        }
    }

    /**
     * {@code similarity=DoT_pRoDuCt} (mixed case) must be stored as {@code DOT_PRODUCT}.
     */
    @Test
    public void testMixedCaseSimilarityIsNormalized() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=DoT_pRoDuCt",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("DOT_PRODUCT", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
        }
    }

    /**
     * An unknown similarity value must be rejected at DDL time with a clear
     * {@link StatementExecutionException} rather than silently stored and only
     * discovered at optimizer-merge time.
     */
    @Test
    public void testUnknownSimilarityRejected() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                                + " WITH m=16 beamWidth=100 similarity=manhattan",
                        Collections.emptyList());
                fail("Expected StatementExecutionException for unknown similarity");
            } catch (StatementExecutionException e) {
                // expected: the message must name the bad value
                String msg = e.getMessage();
                assertNotNull(msg);
                if (!msg.contains("manhattan") && !msg.contains("invalid similarity")) {
                    fail("Exception message must mention the rejected value; got: " + msg);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Issue #520 — default-property materialization
    // -----------------------------------------------------------------------

    /**
     * When the WITH clause is entirely absent the six jvector build parameters
     * must be filled in with canonical defaults.
     */
    @Test
    public void testDefaultsAreMaterializedWhenWithClauseIsAbsent() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("16",        idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("100",       idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            assertEquals("1.2",       idx.properties.get(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW));
            assertEquals("1.4",       idx.properties.get(VectorIndexManager.PROP_ALPHA));
            assertEquals("EUCLIDEAN", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
            assertEquals("false",     idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));
        }
    }

    /**
     * When the WITH clause provides only {@code m} and {@code beamWidth},
     * the remaining jvector parameters must be filled in with defaults.
     */
    @Test
    public void testMissingNeighborOverflowAndAlphaAreFilledWithDefaults() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=32 beamWidth=200 similarity=EUCLIDEAN",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("32",        idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("200",       idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            assertEquals("1.2",       idx.properties.get(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW));
            assertEquals("1.4",       idx.properties.get(VectorIndexManager.PROP_ALPHA));
            assertEquals("EUCLIDEAN", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
        }
    }

    /**
     * User-supplied {@code neighborOverflow} and {@code alpha} must NOT be overridden
     * by defaults — the defaults are only filled in for absent properties.
     */
    @Test
    public void testUserSuppliedNeighborOverflowAndAlphaArePreserved() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=16 beamWidth=100 similarity=EUCLIDEAN"
                            + " neighborOverflow=1.5 alpha=2.0",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("1.5", idx.properties.get(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW));
            assertEquals("2.0", idx.properties.get(VectorIndexManager.PROP_ALPHA));
        }
    }

    /**
     * When the WITH clause provides all six jvector properties explicitly, the stored
     * values must exactly match what the user typed (after similarity normalization).
     */
    @Test
    public void testFullWithClauseStoresAllProperties() throws Exception {
        try (DBManager manager = startManagerWithTable()) {
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)"
                            + " WITH m=8 beamWidth=50 similarity=cosine"
                            + " neighborOverflow=1.3 alpha=1.2 fusedPQ=true",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("8",      idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("50",     idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            assertEquals("COSINE", idx.properties.get(VectorIndexManager.PROP_SIMILARITY));
            assertEquals("1.3",    idx.properties.get(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW));
            assertEquals("1.2",    idx.properties.get(VectorIndexManager.PROP_ALPHA));
            assertEquals("true",   idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));
        }
    }
}
