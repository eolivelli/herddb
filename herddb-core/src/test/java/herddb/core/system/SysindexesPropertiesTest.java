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

package herddb.core.system;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that the {@code properties} column added to {@code sysindexes}
 * (issue #451) surfaces the WITH-clause configuration of every index from any
 * SQL client, with a deterministic JSON encoding.
 */
public class SysindexesPropertiesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void emptyMapEncodesAsEmptyObject() {
        assertEquals("{}", SysindexesTableManager.encodePropertiesAsJson(null));
        assertEquals("{}", SysindexesTableManager.encodePropertiesAsJson(Collections.emptyMap()));
    }

    @Test
    public void keysAreSortedAlphabetically() {
        // Insertion order is z, a, m — encoded order must be a, m, z so that
        // the same input always produces the same JSON output, regardless of
        // the underlying map implementation.
        Map<String, String> in = new LinkedHashMap<>();
        in.put("z", "1");
        in.put("a", "2");
        in.put("m", "3");
        assertEquals("{\"a\":\"2\",\"m\":\"3\",\"z\":\"1\"}",
                SysindexesTableManager.encodePropertiesAsJson(in));
    }

    @Test
    public void encodesTypicalVectorIndexProperties() {
        Map<String, String> in = new TreeMap<>();
        in.put("m", "16");
        in.put("beamWidth", "100");
        in.put("similarity", "euclidean");
        in.put("fusedPQ", "true");
        in.put("numShards", "4");
        // TreeMap is alphabetical; the encoder must produce the same order.
        assertEquals("{\"beamWidth\":\"100\",\"fusedPQ\":\"true\","
                        + "\"m\":\"16\",\"numShards\":\"4\",\"similarity\":\"euclidean\"}",
                SysindexesTableManager.encodePropertiesAsJson(in));
    }

    @Test
    public void specialCharactersAreEscaped() {
        Map<String, String> in = new LinkedHashMap<>();
        in.put("quote", "a\"b");
        in.put("backslash", "a\\b");
        in.put("newline", "a\nb");
        in.put("tab", "a\tb");
        in.put("ctrl", "ab");
        // Keys are sorted: backslash, ctrl, newline, quote, tab.
        String expected = "{\"backslash\":\"a\\\\b\","
                + "\"ctrl\":\"a\\u0001b\","
                + "\"newline\":\"a\\nb\","
                + "\"quote\":\"a\\\"b\","
                + "\"tab\":\"a\\tb\"}";
        assertEquals(expected, SysindexesTableManager.encodePropertiesAsJson(in));
    }

    @Test
    public void sysindexesPropertiesColumnPresentForAllIndexTypes() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        ServerConfiguration config = new ServerConfiguration();
        // Vector index DDL (`CREATE VECTOR INDEX ... WITH ...`) is only
        // accepted by the JSQLParser planner. The default planner would
        // reject the WITH clause entirely.
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null)) {
            manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, cat varchar(20), vec floata not null)",
                    Collections.emptyList());
            // BRIN index — no WITH clause → properties must be the empty
            // JSON object.
            execute(manager, "CREATE BRIN INDEX bidx ON tblspace1.t1 (cat)",
                    Collections.emptyList());
            // Vector index with the full WITH clause → properties must round-
            // trip every key=value pair as JSON, sorted alphabetically.
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1 (vec)"
                            + " WITH m=16 beamWidth=100 similarity=cosine fusedPQ=true numShards=4",
                    Collections.emptyList());

            try (DataScanner scn = scan(manager,
                    "SELECT index_name, index_type, properties FROM tblspace1.sysindexes"
                            + " ORDER BY index_name",
                    Collections.emptyList())) {
                List<DataAccessor> records = scn.consume();
                assertEquals("expected exactly two indexes", 2, records.size());

                DataAccessor brin = records.get(0);
                assertEquals(RawString.of("bidx"), brin.get("index_name"));
                assertEquals(RawString.of("brin"), brin.get("index_type"));
                Object brinProps = brin.get("properties");
                assertNotNull("properties column must not be null for BRIN", brinProps);
                assertEquals(RawString.of("{}"), brinProps);

                DataAccessor vec = records.get(1);
                assertEquals(RawString.of("vidx"), vec.get("index_name"));
                assertEquals(RawString.of("vector"), vec.get("index_type"));
                Object vecProps = vec.get("properties");
                assertNotNull("properties column must not be null for VECTOR", vecProps);
                String vecPropsStr = vecProps.toString();
                // Keys are alphabetical, so the full string is deterministic
                // and we can assert it verbatim — locking down the format so
                // operator dashboards / tests / scripts that parse this column
                // do not have to cope with insertion-order flakes.
                assertEquals(
                        "{\"beamWidth\":\"100\",\"fusedPQ\":\"true\","
                                + "\"m\":\"16\",\"numShards\":\"4\",\"similarity\":\"cosine\"}",
                        vecPropsStr);
                // Belt-and-suspenders: the JSON must contain every WITH-clause
                // value the user typed, regardless of key ordering.
                assertTrue("must contain numShards=4: " + vecPropsStr,
                        vecPropsStr.contains("\"numShards\":\"4\""));
                assertTrue("must contain m=16: " + vecPropsStr,
                        vecPropsStr.contains("\"m\":\"16\""));
                assertTrue("must contain similarity=cosine: " + vecPropsStr,
                        vecPropsStr.contains("\"similarity\":\"cosine\""));
            }
        }
    }
}
