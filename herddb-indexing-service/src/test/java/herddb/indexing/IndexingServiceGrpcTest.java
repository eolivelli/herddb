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

import static org.junit.Assert.*;

import herddb.index.vector.RemoteVectorIndexService;
import herddb.utils.Bytes;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexingServiceGrpcTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;
    private IndexingServiceClient client;

    @Before
    public void setUp() throws Exception {
        java.nio.file.Path logDir = folder.newFolder("log").toPath();
        java.nio.file.Path dataDir = folder.newFolder("data").toPath();
        service = new EmbeddedIndexingService(logDir, dataDir);
        service.start();
        client = service.createClient();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (service != null) {
            service.close();
        }
    }

    @Test
    public void testSearchRoundtrip() {
        float[] vector = new float[]{1.0f, 2.0f, 3.0f};
        List<Map.Entry<Bytes, Float>> results = client.search(
                "default", "mytable", "myindex", vector, 10);
        assertNotNull(results);
        // Engine returns empty results since no data is loaded
        assertTrue(results.isEmpty());
    }

    @Test
    public void testGetIndexStatus() {
        RemoteVectorIndexService.IndexStatusInfo status =
                client.getIndexStatus("default", "mytable", "myindex");
        assertNotNull(status);
        assertNotNull(status.getStatus());
        // The engine reports "tailing" status
        assertEquals("tailing", status.getStatus());
    }
}
