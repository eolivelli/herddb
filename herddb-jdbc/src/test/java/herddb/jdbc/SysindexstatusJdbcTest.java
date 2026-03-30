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

package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SysindexstatusJdbcTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSysindexstatusViaJdbc() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement()) {

                    statement.execute("CREATE TABLE mytable (k1 string primary key, n1 int, s1 string)");
                    statement.execute("CREATE BRIN INDEX brin_idx ON mytable(n1)");
                    statement.execute("CREATE HASH INDEX hash_idx ON mytable(s1)");

                    // Query sysindexstatus
                    try (ResultSet rs = statement.executeQuery("SELECT * FROM sysindexstatus")) {
                        List<String> indexNames = new ArrayList<>();
                        List<String> indexTypes = new ArrayList<>();
                        List<String> properties = new ArrayList<>();
                        while (rs.next()) {
                            indexNames.add(rs.getString("index_name"));
                            indexTypes.add(rs.getString("index_type"));
                            properties.add(rs.getString("properties"));
                            assertNotNull(rs.getString("tablespace"));
                            assertNotNull(rs.getString("table_name"));
                            assertNotNull(rs.getString("index_uuid"));
                        }
                        assertEquals(2, indexNames.size());
                        assertTrue(indexNames.contains("brin_idx"));
                        assertTrue(indexNames.contains("hash_idx"));
                        assertTrue(indexTypes.contains("brin"));
                        assertTrue(indexTypes.contains("hash"));

                        // Check BRIN properties contain numBlocks
                        int brinIdx = indexNames.indexOf("brin_idx");
                        assertTrue(properties.get(brinIdx).contains("\"numBlocks\":"));

                        // Check HASH properties are empty JSON
                        int hashIdx = indexNames.indexOf("hash_idx");
                        assertEquals("{}", properties.get(hashIdx));
                    }

                    // WHERE filter
                    try (ResultSet rs = statement.executeQuery(
                            "SELECT * FROM sysindexstatus WHERE index_type='brin'")) {
                        assertTrue(rs.next());
                        assertEquals("brin_idx", rs.getString("index_name"));
                        assertTrue(rs.getString("properties").contains("\"numBlocks\":"));
                        assertTrue(!rs.next());
                    }

                    // sysindexstatus should appear in systables
                    try (ResultSet rs = statement.executeQuery(
                            "SELECT * FROM SYSTABLES WHERE table_name='sysindexstatus'")) {
                        assertTrue("sysindexstatus should appear in systables", rs.next());
                    }
                }
            }
        }
    }
}
