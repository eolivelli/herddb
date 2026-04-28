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
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.metadata.MetadataStorageManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that CREATE VECTOR INDEX stamps the tablespace's
 * {@code defaultIndexingNumInstances} into the index's
 * {@link VectorIndexManager#PROP_NUM_INSTANCES} property, while preserving
 * any value the user explicitly supplied via {@code WITH (...)}.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexStampsNumInstancesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path data, Path logs, Path meta, Path tmo) throws Exception {
        DBManager m = new DBManager("localhost",
                new FileMetadataStorageManager(meta),
                new FileDataStorageManager(data),
                new FileCommitLogManager(logs),
                tmo, null);
        m.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return m;
    }

    private void createTablespace(DBManager m) throws Exception {
        CreateTableSpaceStatement cs = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        m.executeStatement(cs, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        m.waitForTablespace("tblspace1", 10000);
    }

    private Index getIndex(DBManager m, String name) {
        return m.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get(name).getIndex();
    }

    @Test
    public void stampsDefaultFromTablespaceProperty() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            // Bump the tablespace's default to 4
            MetadataStorageManager msm = m.getMetadataStorageManager();
            TableSpace prev = msm.describeTableSpace("tblspace1");
            TableSpace updated = TableSpace.builder().cloning(prev)
                    .defaultIndexingNumInstances(4).build();
            msm.updateTableSpace(updated, prev);

            execute(m, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(m, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = getIndex(m, "vidx");
            assertEquals("4", idx.properties.get(VectorIndexManager.PROP_NUM_INSTANCES));
        }
    }

    @Test
    public void usesDefaultOneWhenTablespacePropertyAbsent() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            execute(m, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(m, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = getIndex(m, "vidx");
            assertEquals(String.valueOf(TableSpace.DEFAULT_INDEXING_NUM_INSTANCES_DEFAULT),
                    idx.properties.get(VectorIndexManager.PROP_NUM_INSTANCES));
        }
    }

    @Test
    public void explicitUserPropertyWinsOverTablespaceDefault() throws Exception {
        Path data = folder.newFolder("data").toPath();
        Path logs = folder.newFolder("logs").toPath();
        Path meta = folder.newFolder("meta").toPath();
        Path tmo = folder.newFolder("tmo").toPath();

        try (DBManager m = buildManager(data, logs, meta, tmo)) {
            m.start();
            createTablespace(m);

            // Tablespace default = 4, but user explicitly asks for 8 — user wins
            MetadataStorageManager msm = m.getMetadataStorageManager();
            TableSpace prev = msm.describeTableSpace("tblspace1");
            TableSpace updated = TableSpace.builder().cloning(prev)
                    .defaultIndexingNumInstances(4).build();
            msm.updateTableSpace(updated, prev);

            execute(m, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(m, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH numInstances=8",
                    Collections.emptyList());

            Index idx = getIndex(m, "vidx");
            assertEquals("8", idx.properties.get(VectorIndexManager.PROP_NUM_INSTANCES));
        }
    }
}
