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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import herddb.indexing.EmbeddedIndexingService;
import herddb.indexing.IndexingPushClient;
import herddb.indexing.IndexingServerConfiguration;
import herddb.model.TableSpace;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests of the VectorBench {@code --protocol grpc} ingestion path:
 * {@link GrpcBench#ingest} pushes the schema and transactional INSERTs straight
 * into an embedded indexing service running in push mode, and the indexed
 * vector count is verified over gRPC.
 */
class GrpcBenchIntegrationTest {

    @TempDir
    Path tempDir;

    private EmbeddedIndexingService startPushService() throws Exception {
        Path logDir = Files.createDirectories(tempDir.resolve("log"));
        Path dataDir = Files.createDirectories(tempDir.resolve("data"));
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH);
        EmbeddedIndexingService service = new EmbeddedIndexingService(
                logDir, dataDir, new IndexingServerConfiguration(props));
        service.start();
        return service;
    }

    private static Config grpcConfig(String tableName) {
        Config config = new Config();
        config.protocol = Config.Protocol.GRPC;
        config.tableName = tableName;
        config.batchSize = 64;
        config.noProgress = true;
        return config;
    }

    /** Deterministic synthetic base vectors — no dataset download needed. */
    private static Iterator<float[]> syntheticVectors(int count, int dim) {
        return new Iterator<>() {
            private int produced;

            @Override
            public boolean hasNext() {
                return produced < count;
            }

            @Override
            public float[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                float[] v = new float[dim];
                for (int j = 0; j < dim; j++) {
                    v[j] = (produced % 97) + j * 0.5f;
                }
                produced++;
                return v;
            }
        };
    }

    @Test
    @Timeout(120)
    void grpcIngestPushesSchemaAndVectorsAndVerifies() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_bench");
            int rows = 300;
            // ingest() pushes CREATE TABLE + CREATE VECTOR INDEX, then the rows
            // in 64-row transactions, and polls GetIndexStatus until the count
            // matches — it throws if verification fails.
            long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                    syntheticVectors(rows, 16), 0L, rows);
            assertEquals(rows, pushed);
            assertEquals(rows,
                    client.getIndexStatus(TableSpace.DEFAULT, "grpc_bench", "vidx").getVectorCount(),
                    "every pushed vector must be indexed");
        } finally {
            service.close();
        }
    }

    @Test
    @Timeout(120)
    void grpcIngestWithZeroRowsPushesSchemaOnly() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_empty");
            long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                    Collections.<float[]>emptyIterator(), 0L, 0L);
            assertEquals(0, pushed);
            // The schema is still pushed: the index exists and reports 0 vectors.
            assertEquals(0,
                    client.getIndexStatus(TableSpace.DEFAULT, "grpc_empty", "vidx").getVectorCount());
        } finally {
            service.close();
        }
    }

    @Test
    @Timeout(120)
    void grpcIngestWithSkipVerifyReturnsThePushedCount() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_skipverify");
            config.skipVerify = true;
            long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                    syntheticVectors(128, 16), 0L, 128);
            assertEquals(128, pushed);
        } finally {
            service.close();
        }
    }
}
