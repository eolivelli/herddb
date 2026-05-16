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

import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.indexing.IndexingPushClient;
import herddb.indexing.proto.PushEntriesResponse;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

/**
 * The {@code --protocol grpc} ingestion path of VectorBench.
 *
 * <p>Instead of going through JDBC and a HerdDB server, this mode talks
 * straight to a single indexing service running with
 * {@code indexing.log.type=push}: it serializes HerdDB {@link LogEntry}
 * objects itself (CREATE_TABLE, CREATE_INDEX, BEGIN/INSERT/COMMIT) and pushes
 * them over the {@code PushEntries} gRPC RPC. No HerdDB server, BookKeeper or
 * commit log is involved.
 *
 * <p>It is ingestion-only: a transaction wraps each batch of inserts, the
 * entries are serialized into pooled direct {@link ByteBuf}s to keep the
 * client's memory footprint low, and a single thread issues every push so the
 * indexing service sees a strictly increasing LSN stream. Verification polls
 * the index status over gRPC and checks the indexed vector count.
 */
public final class GrpcBench {

    /** Name of the vector index this mode creates, populates and verifies. */
    private static final String INDEX_NAME = "vidx";

    private GrpcBench() {
    }

    /**
     * Entry point for {@code --protocol grpc}. Loads the dataset, opens the
     * gRPC client and drives ingestion, then exits the JVM (mirroring the JDBC
     * path's terminal {@code System.exit(0)}).
     */
    static void run(Config config, BenchOutput out, long benchmarkStartNs) throws Exception {
        out.header("=== gRPC PUSH MODE (indexing service " + config.grpcEndpoint + ") ===");
        if (config.resumeFromAuto) {
            out.info("WARNING: --resume-from auto is not supported with --protocol grpc; "
                    + "ingestion starts at row " + config.resumeFrom + ".");
        }
        out.info("Loading dataset...");
        DatasetLoader loader = new DatasetLoader(config.datasetDir, config.dataset, config.datasetUrl);
        loader.ensureDataset();

        long toIngest = Math.max(0L, config.numRows - config.resumeFrom);
        try (IndexingPushClient client = new IndexingPushClient(config.grpcEndpoint);
                DatasetLoader.VectorStream stream = loader.streamBaseVectors(config.resumeFrom, toIngest)) {
            long pushed = ingest(client, config, out, stream.iterator(), config.resumeFrom, toIngest);
            double totalSecs = (System.nanoTime() - benchmarkStartNs) / 1e9;
            LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
            summary.put("protocol", "grpc");
            summary.put("endpoint", config.grpcEndpoint);
            summary.put("dataset", config.dataset.name());
            summary.put("rows", pushed);
            summary.put("total_wall_time_s", Math.round(totalSecs * 10.0) / 10.0);
            out.summary(summary);
        }
        out.done();
        System.exit(0);
    }

    /**
     * Pushes the schema (CREATE_TABLE + CREATE_INDEX) and then streams
     * {@code totalRows} INSERTs from {@code vectors}, one transaction per
     * {@code config.batchSize} rows, and finally verifies the indexed vector
     * count over gRPC.
     *
     * <p>Package-private and free of {@code System.exit} so integration tests
     * can drive it directly with a synthetic vector source.
     *
     * @return the number of vector rows pushed
     */
    static long ingest(IndexingPushClient client, Config config, BenchOutput out,
                       Iterator<float[]> vectors, long firstRowId, long totalRows) throws Exception {
        Table table = buildTable(config);
        Index index = buildIndex(config);

        // The client owns LSN assignment. A fresh ledger id per run keeps the
        // stream strictly after any watermark a reused indexing service may
        // already hold; offsets are monotonic within the run.
        long ledgerId = System.currentTimeMillis();
        long[] offset = {1L};

        out.phaseStart("schema");
        out.info("Pushing CREATE TABLE " + config.tableName + " + CREATE VECTOR INDEX " + INDEX_NAME);
        PushEntriesResponse schemaResp = pushBatch(client, ledgerId, offset, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(index, null)));
        if (schemaResp.getAcceptedCount() != 2) {
            throw new IllegalStateException("indexing service accepted "
                    + schemaResp.getAcceptedCount() + " of 2 schema entries");
        }
        // Baseline so verification works even against a non-fresh service.
        long baseline = client.getIndexStatus(TableSpace.DEFAULT, config.tableName, INDEX_NAME)
                .getVectorCount();
        out.phaseDone("schema", 0.0);

        out.header("=== INGESTION PHASE (gRPC push) ===");
        out.phaseStart("ingest");
        long ingestStart = System.nanoTime();
        long rowId = firstRowId;
        long pushed = 0;
        long pushCalls = 0;
        long txId = 1;
        long lastProgressNs = ingestStart;

        while (vectors.hasNext()) {
            // One transaction per push batch: BEGIN + N INSERTs + COMMIT.
            List<LogEntry> batch = new ArrayList<>(config.batchSize + 2);
            batch.add(LogEntryFactory.beginTransaction(txId));
            int n = 0;
            while (n < config.batchSize && vectors.hasNext()) {
                float[] v = vectors.next();
                Record record = RecordSerializer.makeRecord(table, "id", rowId, "vec", v);
                batch.add(new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT,
                        txId, table.tableId, record.key, record.value));
                rowId++;
                n++;
            }
            batch.add(LogEntryFactory.commitTransaction(txId));
            txId++;

            PushEntriesResponse resp = pushBatch(client, ledgerId, offset, batch);
            pushed += n;
            pushCalls++;
            if (resp.getAcceptedCount() != batch.size()) {
                throw new IllegalStateException("indexing service accepted "
                        + resp.getAcceptedCount() + " of " + batch.size() + " pushed entries");
            }

            long now = System.nanoTime();
            if (now - lastProgressNs > 1_000_000_000L || !vectors.hasNext()) {
                lastProgressNs = now;
                double elapsed = (now - ingestStart) / 1e9;
                double opsPerSec = elapsed > 0 ? pushed / elapsed : 0.0;
                LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
                fields.put("rows", pushed);
                fields.put("total", totalRows);
                fields.put("push_calls", pushCalls);
                fields.put("ops_per_sec", opsPerSec);
                out.progress("ingest", elapsed, String.format(
                        "pushed %d/%d rows | %.0f ops/s | %d push calls",
                        pushed, totalRows, opsPerSec, pushCalls), fields);
            }
        }

        double ingestSecs = (System.nanoTime() - ingestStart) / 1e9;
        out.phaseDone("ingest", ingestSecs);
        out.info(String.format("Pushed %d rows in %d transactions over %.1fs (%.0f ops/s)",
                pushed, pushCalls, ingestSecs, ingestSecs > 0 ? pushed / ingestSecs : 0.0));

        if (config.skipVerify) {
            out.info("Skipping vector-count verification (--skip-verify).");
        } else {
            verifyVectorCount(client, config, out, baseline + pushed);
        }
        return pushed;
    }

    /**
     * Polls {@code GetIndexStatus} until the indexed vector count reaches
     * {@code expected}. The indexing service applies pushed entries
     * asynchronously and may pause for a checkpoint/compaction, so this waits
     * with a generous deadline rather than asserting immediately.
     */
    private static void verifyVectorCount(IndexingPushClient client, Config config,
                                          BenchOutput out, long expected) throws InterruptedException {
        out.phaseStart("verification");
        long verifyStart = System.nanoTime();
        long deadlineMs = System.currentTimeMillis() + 3_600_000L;
        long last = -1;
        while (System.currentTimeMillis() < deadlineMs) {
            last = client.getIndexStatus(TableSpace.DEFAULT, config.tableName, INDEX_NAME)
                    .getVectorCount();
            if (last >= expected) {
                out.phaseDone("verification", (System.nanoTime() - verifyStart) / 1e9);
                out.info("Verification OK: index '" + INDEX_NAME + "' reports " + last + " vectors");
                return;
            }
            double elapsed = (System.nanoTime() - verifyStart) / 1e9;
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            fields.put("vector_count", last);
            fields.put("expected", expected);
            out.progress("verification", elapsed,
                    "indexed " + last + "/" + expected + " vectors", fields);
            Thread.sleep(1000);
        }
        throw new IllegalStateException("index vector count reached only " + last
                + " of the expected " + expected + " within the verification timeout");
    }

    /**
     * Pushes a batch, serializing each entry with
     * {@link LogEntry#serializeAsByteBuf()}.
     */
    private static PushEntriesResponse pushBatch(IndexingPushClient client, long ledgerId,
                                                 long[] offset, List<LogEntry> entries) {
        return pushBatch(client, ledgerId, offset, entries, LogEntry::serializeAsByteBuf);
    }

    /**
     * Serializes each entry into a pooled direct {@link ByteBuf} with
     * {@code serializer}, assigns it the next LSN, pushes the batch, and
     * releases <em>every</em> buffer once the (zero-copy) RPC has returned —
     * including when serializing a later entry throws mid-batch.
     *
     * <p>The serializer is a parameter so tests can exercise that buffer
     * lifecycle without a live indexing service.
     */
    static PushEntriesResponse pushBatch(IndexingPushClient client, long ledgerId, long[] offset,
                                         List<LogEntry> entries,
                                         Function<LogEntry, ByteBuf> serializer) {
        List<LogSequenceNumber> lsns = new ArrayList<>(entries.size());
        List<ByteBuf> bufs = new ArrayList<>(entries.size());
        try {
            for (LogEntry entry : entries) {
                lsns.add(new LogSequenceNumber(ledgerId, offset[0]++));
                bufs.add(serializer.apply(entry));
            }
            return client.pushEntries(lsns, bufs);
        } finally {
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        }
    }

    private static Table buildTable(Config config) {
        return Table.builder()
                .name(config.tableName)
                .tablespace(TableSpace.DEFAULT)
                // LONG, not INTEGER: a bench may ingest more than 2^31 rows and
                // the primary key must stay unique.
                .column("id", ColumnTypes.LONG)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("id")
                .build();
    }

    private static Index buildIndex(Config config) {
        Index.Builder builder = Index.builder()
                .name(INDEX_NAME)
                .table(config.tableName)
                .tablespace(TableSpace.DEFAULT)
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, String.valueOf(config.indexM))
                .property(VectorIndexManager.PROP_BEAM_WIDTH, String.valueOf(config.indexBeamWidth))
                .property(VectorIndexManager.PROP_SIMILARITY, config.effectiveSimilarity())
                .property(VectorIndexManager.PROP_FUSED_PQ, "true")
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW,
                        String.valueOf(config.indexNeighborOverflow))
                .property(VectorIndexManager.PROP_ALPHA, String.valueOf(config.indexAlpha));
        if (config.indexNumShards > 1) {
            builder.property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(config.indexNumShards));
        }
        return builder.build();
    }
}
