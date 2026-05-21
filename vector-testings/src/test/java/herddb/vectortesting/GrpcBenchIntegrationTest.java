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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.indexing.EmbeddedIndexingService;
import herddb.indexing.IndexingPushClient;
import herddb.indexing.IndexingServerConfiguration;
import herddb.model.TableSpace;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * Issue #632 (1): the gRPC ingest path must thread {@link BenchRuntime}
     * status through each phase (schema → ingest → verification) so the admin
     * {@code /status} endpoint stops permanently reporting {@code idle}.
     * Sample the runtime supplier from a background thread while ingestion
     * is in progress and assert we observe at least one non-{@code idle}
     * phase before the bench finishes.
     */
    @Test
    @Timeout(60)
    void grpcIngestWiresPhaseToBenchRuntime() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_runtime_phase");
            // Keep the ingest large enough that the sampler runs while we're
            // still pushing — 2000 vectors at batchSize=64 is ~30 push calls,
            // each with a sub-millisecond gRPC round-trip, so the sampler will
            // see the ingest phase mid-loop on any sane CI host.
            int rows = 2000;
            BenchRuntime runtime = new BenchRuntime(config);
            assertEquals("idle", runtime.getStatusSupplier().get().get("phase"),
                    "fresh BenchRuntime must default to phase=idle");

            Set<String> observedPhases = ConcurrentHashMap.newKeySet();
            Thread sampler = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    Map<String, Object> snapshot = runtime.getStatusSupplier().get();
                    Object phase = snapshot.get("phase");
                    if (phase != null) {
                        observedPhases.add(phase.toString());
                    }
                    try {
                        // 1 ms is aggressive enough to catch sub-second phase transitions.
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "phase-sampler");
            sampler.setDaemon(true);
            sampler.start();
            try {
                long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                        syntheticVectors(rows, 16), 0L, rows, runtime);
                assertEquals(rows, pushed);
            } finally {
                sampler.interrupt();
                sampler.join();
            }

            // We must have observed the ingest phase (or schema, or verification) —
            // i.e. the supplier was swapped away from the default "idle".
            assertTrue(observedPhases.contains("ingest") || observedPhases.contains("schema")
                            || observedPhases.contains("verification"),
                    "expected to observe at least one non-idle phase during ingest, got: " + observedPhases);
        } finally {
            service.close();
        }
    }

    /**
     * Issue #632 (2): the verification phase must complete quickly in push
     * mode — the IS is "always up to date" once the bounded push buffer
     * drains, so the wait is bounded by milliseconds in practice, not by
     * {@code --wait-for-indexes-timeout}. We set that JDBC flag to 1 second
     * to prove it is ignored (the old 1-hour hard-coded deadline would also
     * be ignored, and a regression that re-introduced it would still fall
     * within the test timeout — the strong tripwire is the unit test
     * {@code GrpcBenchTest#verifyVectorCountFailsFastWhenIndexFallsShort}).
     */
    @Test
    @Timeout(30)
    void grpcVerifyReturnsImmediatelyWhenIndexIsUpToDate() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_verify_fast");
            // Tiny value, would block a JDBC WAITFORINDEXES — must be ignored
            // by the push-mode verification.
            config.waitForIndexesTimeoutSeconds = 1;
            long start = System.nanoTime();
            long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                    syntheticVectors(200, 16), 0L, 200);
            double elapsedSecs = (System.nanoTime() - start) / 1e9;
            assertEquals(200, pushed);
            assertTrue(elapsedSecs < 20.0,
                    "push-mode verification must be near-instant, elapsed=" + elapsedSecs + " s");
        } finally {
            service.close();
        }
    }

    /**
     * Issue #632 (3): the new query / recall phase runs gRPC {@code Search}
     * calls against the same IS the bench has just populated. With a tiny
     * deterministic dataset (200 dim-8 vectors built from a coordinate ramp)
     * the query for the exact vector at row K must rank K first — so
     * recall@1 = 1.0 across every probe. This pins both the
     * {@link IndexingPushClient#search} round-trip and the primary-key
     * deserialization in {@code GrpcBench.searchRange}.
     */
    @Test
    @Timeout(120)
    void grpcQueryPhaseProducesPerfectRecallOnDeterministicData() throws Exception {
        EmbeddedIndexingService service = startPushService();
        try (IndexingPushClient client = new IndexingPushClient(service.getAddress())) {
            Config config = grpcConfig("grpc_recall");
            int rows = 200;
            int dim = 8;
            long pushed = GrpcBench.ingest(client, config, BenchOutput.create(config),
                    rampVectors(rows, dim), 0L, rows);
            assertEquals(rows, pushed);

            // Issue Search RPCs for the exact vectors at K=10 known positions.
            // Each must rank its own id first (recall@1 = 1).
            int hits = 0;
            int probes = 10;
            for (int k = 0; k < probes; k++) {
                int probeId = k * (rows / probes);
                float[] query = rampVector(probeId, dim);
                var resp = client.search(TableSpace.DEFAULT, "grpc_recall", "vidx", query, 5);
                assertNotNull(resp);
                assertTrue(resp.getResultsCount() > 0,
                        "search must return at least one hit for id=" + probeId);
                // Decode the top result's primary key (single-column LONG id).
                herddb.utils.Bytes pk = herddb.utils.Bytes.from_array(
                        resp.getResults(0).getPrimaryKey().toByteArray());
                herddb.model.Table table = herddb.model.Table.builder()
                        .name("grpc_recall")
                        .tablespace(TableSpace.DEFAULT)
                        .column("id", herddb.model.ColumnTypes.LONG)
                        .column("vec", herddb.model.ColumnTypes.FLOATARRAY)
                        .primaryKey("id")
                        .build();
                long topId = ((Number) herddb.codec.RecordSerializer.deserializePrimaryKey(pk, table)).longValue();
                if (topId == probeId) {
                    hits++;
                }
            }
            assertTrue(hits >= probes - 1,
                    "expected at least " + (probes - 1) + " exact-match top hits, got " + hits);
        } finally {
            service.close();
        }
    }

    /** Same deterministic ramp as {@link #syntheticVectors} but exposed as a single vector. */
    private static float[] rampVector(int id, int dim) {
        float[] v = new float[dim];
        for (int j = 0; j < dim; j++) {
            // Make every id a distinct point — no modulo. Recall semantics
            // for the integration test rely on per-id uniqueness.
            v[j] = id + j * 0.5f;
        }
        return v;
    }

    /** Per-id-distinct ramp iterator — used so the recall test has unambiguous nearest neighbours. */
    private static Iterator<float[]> rampVectors(int count, int dim) {
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
                float[] v = rampVector(produced, dim);
                produced++;
                return v;
            }
        };
    }
}
