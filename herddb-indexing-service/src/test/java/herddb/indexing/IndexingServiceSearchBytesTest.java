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

import static org.junit.Assert.assertTrue;

import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for IndexingServiceImpl search_bytes counter.
 * Verifies that search response bytes are being tracked in metrics.
 */
public class IndexingServiceSearchBytesTest {

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
    public void testSearchBytesMetricPresent() throws Exception {
        // Create a simple search request
        SearchRequest.Builder requestBuilder = SearchRequest.newBuilder()
                .setTablespace("test_space")
                .setTable("test_table")
                .setIndex("test_index")
                .setLimit(10)
                .setReturnScore(true);

        // Add a query vector
        for (int i = 0; i < 10; i++) {
            requestBuilder.addVector((float) i);
        }

        SearchRequest request = requestBuilder.build();

        // Execute search
        CountDownLatch latch = new CountDownLatch(1);
        final SearchResponse[] response = new SearchResponse[1];

        service.getStub().search(request, new StreamObserver<SearchResponse>() {
            @Override
            public void onNext(SearchResponse value) {
                response[0] = value;
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        // Wait for response
        assertTrue("Search should complete within 5 seconds",
                latch.await(5, TimeUnit.SECONDS));

        // Verify metrics endpoint shows search_bytes counter
        String metricsOutput = getMetrics();
        assertTrue("indexing_search_bytes metric should be present",
                metricsOutput.contains("indexing_search_bytes"));
    }

    private String getMetrics() throws Exception {
        java.net.URL metricsUrl = new java.net.URL(
                "http://localhost:" + service.getHttpPort() + "/metrics");
        try (java.util.Scanner scanner = new java.util.Scanner(metricsUrl.openStream())) {
            return scanner.useDelimiter("\\A").next();
        }
    }
}
