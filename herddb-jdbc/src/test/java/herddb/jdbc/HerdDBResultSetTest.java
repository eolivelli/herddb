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

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HerdDBResultSetTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * Regression test for issue #365: getString() on a floatarray column must return a human-readable
     * "[v0, v1, ...]" string instead of the Java identity hashcode "[F@&lt;hex&gt;".
     */
    @Test
    public void getStringOnFloatArrayColumnReturnsReadableRepresentation() throws Exception {
        try (final Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            try (final HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try (final BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client)) {

                    try (Connection con = dataSource.getConnection();
                         Statement statement = con.createStatement()) {
                        statement.execute("CREATE TABLE vectable (id int primary key, vec floatarray)");
                    }

                    float[] vector = {0.1f, 0.2f, 0.3f};
                    try (Connection con = dataSource.getConnection();
                         PreparedStatement ps = con.prepareStatement("INSERT INTO vectable VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setObject(2, vector);
                        ps.executeUpdate();
                    }

                    try (Connection con = dataSource.getConnection();
                         PreparedStatement ps = con.prepareStatement("SELECT id, vec FROM vectable WHERE id = ?")) {
                        ps.setInt(1, 1);
                        try (ResultSet rs = ps.executeQuery()) {
                            Assert.assertTrue("Expected one row", rs.next());

                            // getString() must not return "[F@<hashcode>" — it must be readable.
                            String vecStr = rs.getString("vec");
                            Assert.assertNotNull(vecStr);
                            Assert.assertFalse(
                                    "getString() on floatarray must not return an object reference (got: " + vecStr + ")",
                                    vecStr.startsWith("[F@")
                            );

                            // The string must be the Arrays.toString() representation.
                            String expected = Arrays.toString(vector);
                            Assert.assertEquals(expected, vecStr);

                            Assert.assertFalse("Expected exactly one row", rs.next());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void getStatement() throws Exception {
        try (final Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            try (final HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try (final BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client)) {

                    try (Connection con = dataSource.getConnection();
                         Statement statement = con.createStatement()) {
                        statement.execute("CREATE TABLE mytable (c1 int primary key)");
                    }

                    try (final Connection con = dataSource.getConnection();
                         final PreparedStatement statement = con.prepareStatement("select * from mytable");
                         final ResultSet rows = statement.executeQuery()) {

                        Assert.assertEquals(statement, rows.getStatement());
                    }

                }
            }
        }
    }
}
