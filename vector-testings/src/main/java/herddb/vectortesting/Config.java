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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Config {

    public enum OutputFormat {
        TEXT,
        JSON
    }

    /** Wire protocol used by the benchmark. */
    public enum Protocol {
        JDBC,
        GRPC
    }

    String jdbcUrl = "jdbc:herddb:server:localhost:7000";
    String username = "sa";
    String password = "hdb";
    String tableName = "vector_bench";
    String datasetDir = "./datasets";
    String datasetUrl = null; // null means use preset default
    DatasetLoader.DatasetPreset dataset = DatasetLoader.DatasetPreset.SIFT1M;
    long numRows = 100_000L;
    int ingestThreads = 4;
    /**
     * Rows per JDBC {@code executeBatch()} flush. {@code volatile} so a
     * value updated from a Jetty admin handler thread is visible to ingest
     * worker threads on their next per-row check.
     */
    volatile int batchSize = 500;
    /**
     * Rows per JDBC commit (a "transaction"). When {@code 0} (the default),
     * the worker uses {@link #batchSize} as the commit unit — preserving the
     * pre-issue-#401 behaviour of one flush per commit. When set to a value
     * {@code >= batchSize}, each commit accumulates multiple
     * {@code executeBatch()} flushes on the same JDBC connection before
     * committing once at the transaction boundary. {@code volatile} for
     * cross-thread visibility from the admin API.
     */
    volatile int transactionSize = 0;
    int queryThreads = 4;
    int queryCount = 1000;
    volatile int topK = 10;
    boolean topKExplicit = false;
    int indexM = 16;
    int indexBeamWidth = 100;
    int indexNumShards = 4;
    /**
     * jvector {@code neighborOverflow} build parameter.  Controls how
     * aggressively the graph builder admits slightly-suboptimal edges during
     * construction.  Default 1.2 matches the jvector {@code GraphIndexBuilder}
     * default.  Always emitted in the {@code CREATE VECTOR INDEX} DDL so the
     * optimizer can read it from the index metadata (issue #520).
     */
    float indexNeighborOverflow = 1.2f;
    /**
     * jvector {@code alpha} build parameter.  Scales the minimum-spanning-tree
     * pruning criterion.  Default 1.4 matches the jvector
     * {@code GraphIndexBuilder} default.  Always emitted in the
     * {@code CREATE VECTOR INDEX} DDL so the optimizer can read it from the
     * index metadata (issue #520).
     */
    float indexAlpha = 1.4f;
    boolean skipIngest = false;
    boolean skipIndex = false;
    boolean skipVerify = false;
    /**
     * Skip the post-ingest query / recall phase. Honored only by
     * {@code GrpcBench} ({@code --protocol grpc}); the JDBC path has its own
     * skip semantics keyed on {@code skipIngest} / {@code skipVerify}. When
     * {@code true}, gRPC mode exits right after count verification and emits
     * the bench summary without any {@code Search} RPCs or recall metric.
     */
    boolean skipQuery = false;
    boolean dropTable = false;
    boolean checkpoint = false;
    int clientTimeoutSeconds = 7200 * 4; // 8 hours
    String similarity = null; // null = use dataset default
    boolean indexBeforeIngest = true;
    long resumeFrom = 0L; // skip first N vectors; row IDs start from N
    boolean resumeFromAuto = false; // when true, resolve resumeFrom from SELECT COUNT(*) before ingest
    volatile int ingestMaxOpsPerSecond = 100_000; // 0 = unlimited
    volatile int queryMaxOpsPerSecond = 10; // 0 = unlimited
    int ingestCommitRetries = 3;
    int checkpointTimeoutSeconds = 300;
    boolean waitForIndexes = false;
    int waitForIndexesTimeoutSeconds = 600;
    boolean noProgress = false;
    OutputFormat outputFormat = OutputFormat.TEXT;
    /**
     * Wire protocol: {@link Protocol#JDBC} (default) drives the benchmark
     * through a HerdDB server; {@link Protocol#GRPC} pushes serialized
     * {@code LogEntry} objects straight into a single indexing service
     * ({@code indexing.log.type=push}) — ingestion only.
     */
    Protocol protocol = Protocol.JDBC;
    /**
     * Indexing-service gRPC endpoint ({@code host:port}) used when
     * {@link #protocol} is {@link Protocol#GRPC}.
     */
    String grpcEndpoint = "localhost:9850";
    /**
     * Interval in seconds between periodic {@code [status]} dumps during ingestion.
     * A dedicated JDBC connection queries {@code syslogstatus}, {@code systablestats} and
     * {@code sysindexstatus} every {@code N} seconds so checkpoint / index-tail lag is
     * visible in the run log. {@code 0} disables the status thread entirely.
     */
    int statusIntervalSeconds = 60;
    /**
     * When {@code true}, a dedicated background thread runs the configured query
     * workload periodically throughout the ingestion phase, using a separate JDBC
     * connection opened in autocommit mode (independent of all ingest worker
     * connections). Recall is intentionally <em>not</em> computed during these
     * mid-ingestion rounds because the ground-truth file covers the full dataset,
     * not the partially ingested one.
     */
    boolean runQueriesDuringIngestion = false;
    /**
     * Period in seconds between consecutive query rounds when
     * {@link #runQueriesDuringIngestion} is {@code true}. Must be {@code >= 1}.
     * Default: {@code 30}.
     */
    int runQueriesDuringIngestionPeriodSeconds = 30;

    private static Options buildOptions() {
        Options opts = new Options();
        opts.addOption("u", "url", true, "JDBC URL (default: jdbc:herddb:server:localhost:7000)");
        opts.addOption(null, "user", true, "Username (default: sa)");
        opts.addOption(null, "password", true, "Password (default: hdb)");
        opts.addOption(null, "table", true, "Table name (default: vector_bench)");
        opts.addOption(null, "dataset-dir", true, "Dataset download/cache directory (default: $VECTORBENCH_DATASET_DIR or ./datasets)");
        opts.addOption(null, "dataset", true, "Dataset preset: sift10k, sift1m, gist1m, sift10m, bigann, glove100, deep-image-96 (default: sift1m)");
        opts.addOption(null, "dataset-url", true, "Override dataset download URL");
        opts.addOption("n", "rows", true, "Number of rows to ingest (default: 100000, cycles dataset if larger)");
        opts.addOption(null, "ingest-threads", true, "Ingestion parallelism (default: 4)");
        opts.addOption(null, "batch-size", true,
                "Rows per JDBC executeBatch() flush (default: 500). When --transaction-size "
                        + "is unset, this is also the commit unit (one flush per commit, "
                        + "preserving the legacy behaviour). Runtime-tunable via the admin API.");
        opts.addOption(null, "transaction-size", true,
                "Rows per JDBC commit (default: same as --batch-size). When set, each commit "
                        + "accumulates multiple --batch-size flushes on the same JDBC connection "
                        + "before committing once at the transaction boundary. Must be >= --batch-size "
                        + "and <= --ingest-max-ops (when finite). Runtime-tunable via the admin API.");
        opts.addOption(null, "query-threads", true, "Query parallelism (default: 4)");
        opts.addOption(null, "queries", true, "Number of ANN queries to execute (default: 1000)");
        opts.addOption("k", null, true, "LIMIT K for ANN queries (default: 10)");
        opts.addOption(null, "m", true, "Vector index M parameter (default: 16)");
        opts.addOption(null, "beam-width", true, "Vector index beamWidth (default: 100)");
        opts.addOption(null, "index-num-shards", true,
                "Vector index numShards (default: 4). Set to 1 to disable sharding; "
                        + "otherwise emitted as `numShards=N` in the CREATE VECTOR INDEX DDL.");
        opts.addOption(null, "neighbor-overflow", true,
                "jvector neighborOverflow build parameter (default: 1.2). "
                        + "Always emitted in the CREATE VECTOR INDEX DDL so the optimizer can read it.");
        opts.addOption(null, "alpha", true,
                "jvector alpha build parameter (default: 1.4). "
                        + "Always emitted in the CREATE VECTOR INDEX DDL so the optimizer can read it.");
        opts.addOption(null, "skip-ingest", false, "Skip ingestion phase");
        opts.addOption(null, "skip-index", false, "Skip index creation");
        opts.addOption(null, "skip-verify", false, "Skip row count verification after ingestion");
        opts.addOption(null, "skip-query", false,
                "Skip the post-ingest query / recall phase (--protocol grpc only). "
                        + "Use for pure ingestion benchmarks where recall is computed separately.");
        opts.addOption(null, "drop-table", false, "Drop table before starting");
        opts.addOption(null, "checkpoint", false, "Force checkpoint after ingestion and after index creation");
        opts.addOption(null, "similarity", true, "Similarity function: euclidean, cosine, dot_product (default: from dataset)");
        opts.addOption(null, "client-timeout", true, "Client request timeout in seconds (default: 7200)");
        opts.addOption(null, "index-before-ingest", false, "Create vector index before ingestion instead of after");
        opts.addOption(null, "resume-from", true,
                "Skip first N vectors and start row IDs from N, or 'auto' to query MAX(id)+1 from the table (default: 0)");
        opts.addOption(null, "ingest-max-ops", true, "Max ingestion ops/s across all threads, 0=unlimited (default: 100000)");
        opts.addOption(null, "query-max-ops", true, "Max query ops/s across all threads, 0=unlimited (default: 10)");
        opts.addOption(null, "ingest-commit-retries", true,
                "Retries per failed batch commit before failing the run (default: 3, "
                        + "exponential back-off 10s/20s/40s...)");
        opts.addOption(null, "checkpoint-timeout-seconds", true, "Seconds to wait for the Indexing Service to catch up during --checkpoint (default: 300)");
        opts.addOption(null, "wait-for-indexes", false,
                "Before running queries, run EXECUTE WAITFORINDEXES to block until all external tailers (indexing services) have caught up. "
                        + "Required for reliable recall numbers when tailers are in use.");
        opts.addOption(null, "wait-for-indexes-timeout", true,
                "Seconds to wait for external tailers to catch up during --wait-for-indexes (default: 600)");
        opts.addOption(null, "no-progress", false,
                "Disable animated spinner; emit one plain \\n-terminated line per progress sample "
                        + "(implicitly enabled when VECTOR_BENCH_NO_PROGRESS=1 or --output-format=json)");
        opts.addOption(null, "output-format", true,
                "Output format: text (default) or json (NDJSON, one object per line). json implies --no-progress.");
        opts.addOption(null, "status-interval-seconds", true,
                "Seconds between server-status dumps during ingestion; 0 disables (default: 60)");
        opts.addOption(null, "run-queries-during-ingestion", false,
                "Run the configured query workload periodically while ingestion is in progress. "
                        + "Queries use a separate autocommit connection. Recall is not computed "
                        + "mid-ingestion (ground truth covers the full dataset, not a partial one).");
        opts.addOption(null, "run-queries-during-ingestion-period", true,
                "Seconds between consecutive query rounds when --run-queries-during-ingestion is "
                        + "active (default: 30, minimum: 1)");
        opts.addOption(null, "protocol", true,
                "Wire protocol: jdbc (default) or grpc. grpc pushes serialized LogEntries "
                        + "straight into a single indexing service (ingestion only).");
        opts.addOption(null, "grpc-endpoint", true,
                "Indexing-service gRPC endpoint host:port for --protocol grpc "
                        + "(default: localhost:9850)");
        opts.addOption(null, "config", true, "Path to properties file");
        opts.addOption("h", "help", false, "Show help");
        return opts;
    }

    static Config parse(String[] args) throws ParseException {
        Options opts = buildOptions();
        CommandLine cmd = new DefaultParser().parse(opts, args);

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("vector-bench", opts);
            System.exit(0);
        }

        Config cfg = new Config();

        // Load properties file first (CLI flags override)
        if (cmd.hasOption("config")) {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(cmd.getOptionValue("config"))) {
                props.load(fis);
            } catch (IOException e) {
                throw new ParseException("Cannot read config file: " + e.getMessage());
            }
            cfg.applyProperties(props);
        }

        // CLI overrides
        if (cmd.hasOption("url")) {
            cfg.jdbcUrl = cmd.getOptionValue("url");
        }
        if (cmd.hasOption("protocol")) {
            cfg.protocol = parseProtocol(cmd.getOptionValue("protocol"));
        }
        if (cmd.hasOption("grpc-endpoint")) {
            cfg.grpcEndpoint = cmd.getOptionValue("grpc-endpoint");
        }
        if (cmd.hasOption("user")) {
            cfg.username = cmd.getOptionValue("user");
        }
        if (cmd.hasOption("password")) {
            cfg.password = cmd.getOptionValue("password");
        }
        if (cmd.hasOption("table")) {
            cfg.tableName = cmd.getOptionValue("table");
        }
        if (cmd.hasOption("dataset-dir")) {
            cfg.datasetDir = cmd.getOptionValue("dataset-dir");
        }
        if (cmd.hasOption("dataset")) {
            cfg.dataset = parseDataset(cmd.getOptionValue("dataset"));
        }
        if (cmd.hasOption("dataset-url")) {
            cfg.datasetUrl = cmd.getOptionValue("dataset-url");
        }
        if (cmd.hasOption("rows")) {
            cfg.numRows = Long.parseLong(cmd.getOptionValue("rows"));
        }
        if (cmd.hasOption("ingest-threads")) {
            cfg.ingestThreads = Integer.parseInt(cmd.getOptionValue("ingest-threads"));
        }
        if (cmd.hasOption("batch-size")) {
            cfg.batchSize = Integer.parseInt(cmd.getOptionValue("batch-size"));
        }
        if (cmd.hasOption("transaction-size")) {
            cfg.transactionSize = Integer.parseInt(cmd.getOptionValue("transaction-size"));
        }
        if (cmd.hasOption("query-threads")) {
            cfg.queryThreads = Integer.parseInt(cmd.getOptionValue("query-threads"));
        }
        if (cmd.hasOption("queries")) {
            cfg.queryCount = Integer.parseInt(cmd.getOptionValue("queries"));
        }
        if (cmd.hasOption("k")) {
            cfg.topK = Integer.parseInt(cmd.getOptionValue("k"));
            cfg.topKExplicit = true;
        }
        if (cmd.hasOption("m")) {
            cfg.indexM = Integer.parseInt(cmd.getOptionValue("m"));
        }
        if (cmd.hasOption("beam-width")) {
            cfg.indexBeamWidth = Integer.parseInt(cmd.getOptionValue("beam-width"));
        }
        if (cmd.hasOption("index-num-shards")) {
            cfg.indexNumShards = Integer.parseInt(cmd.getOptionValue("index-num-shards"));
        }
        if (cmd.hasOption("neighbor-overflow")) {
            cfg.indexNeighborOverflow = Float.parseFloat(cmd.getOptionValue("neighbor-overflow"));
        }
        if (cmd.hasOption("alpha")) {
            cfg.indexAlpha = Float.parseFloat(cmd.getOptionValue("alpha"));
        }
        if (cmd.hasOption("skip-ingest")) {
            cfg.skipIngest = true;
        }
        if (cmd.hasOption("skip-index")) {
            cfg.skipIndex = true;
        }
        if (cmd.hasOption("skip-verify")) {
            cfg.skipVerify = true;
        }
        if (cmd.hasOption("skip-query")) {
            cfg.skipQuery = true;
        }
        if (cmd.hasOption("drop-table")) {
            cfg.dropTable = true;
        }
        if (cmd.hasOption("checkpoint")) {
            cfg.checkpoint = true;
        }
        if (cmd.hasOption("similarity")) {
            cfg.similarity = cmd.getOptionValue("similarity");
        }
        if (cmd.hasOption("client-timeout")) {
            cfg.clientTimeoutSeconds = Integer.parseInt(cmd.getOptionValue("client-timeout"));
        }
        if (cmd.hasOption("index-before-ingest")) {
            cfg.indexBeforeIngest = true;
        }
        if (cmd.hasOption("resume-from")) {
            parseResumeFrom(cfg, cmd.getOptionValue("resume-from"));
        }
        if (cmd.hasOption("ingest-max-ops")) {
            cfg.ingestMaxOpsPerSecond = Integer.parseInt(cmd.getOptionValue("ingest-max-ops"));
        }
        if (cmd.hasOption("query-max-ops")) {
            cfg.queryMaxOpsPerSecond = Integer.parseInt(cmd.getOptionValue("query-max-ops"));
        }
        if (cmd.hasOption("ingest-commit-retries")) {
            cfg.ingestCommitRetries = Integer.parseInt(cmd.getOptionValue("ingest-commit-retries"));
        }
        if (cmd.hasOption("checkpoint-timeout-seconds")) {
            cfg.checkpointTimeoutSeconds = Integer.parseInt(cmd.getOptionValue("checkpoint-timeout-seconds"));
        }
        if (cmd.hasOption("wait-for-indexes")) {
            cfg.waitForIndexes = true;
        }
        if (cmd.hasOption("wait-for-indexes-timeout")) {
            cfg.waitForIndexesTimeoutSeconds = Integer.parseInt(cmd.getOptionValue("wait-for-indexes-timeout"));
        }
        if (cmd.hasOption("no-progress")) {
            cfg.noProgress = true;
        }
        if (cmd.hasOption("output-format")) {
            cfg.outputFormat = parseOutputFormat(cmd.getOptionValue("output-format"));
        }
        if (cmd.hasOption("status-interval-seconds")) {
            cfg.statusIntervalSeconds = Integer.parseInt(cmd.getOptionValue("status-interval-seconds"));
        }
        if (cmd.hasOption("run-queries-during-ingestion")) {
            cfg.runQueriesDuringIngestion = true;
        }
        if (cmd.hasOption("run-queries-during-ingestion-period")) {
            cfg.runQueriesDuringIngestionPeriodSeconds = Integer.parseInt(
                    cmd.getOptionValue("run-queries-during-ingestion-period"));
        }

        // Validate batch/transaction/ingest-max-ops invariants. We surface these as
        // ParseException because they are user-supplied configuration errors and
        // the rest of parse() also throws ParseException for input issues.
        validateBatchAndTransactionInvariants(cfg);
        validateIndexBuildParams(cfg);

        // Env var fallbacks (only applied when the CLI flag was not set).
        if (!cmd.hasOption("no-progress") && !cfg.noProgress) {
            String envNoProgress = System.getenv("VECTOR_BENCH_NO_PROGRESS");
            if (isTruthy(envNoProgress)) {
                cfg.noProgress = true;
            }
        }
        if (!cmd.hasOption("output-format") && cfg.outputFormat == OutputFormat.TEXT) {
            String envFmt = System.getenv("VECTOR_BENCH_OUTPUT_FORMAT");
            if (envFmt != null && !envFmt.isEmpty()) {
                cfg.outputFormat = parseOutputFormat(envFmt);
            }
        }

        // JSON output implies --no-progress: there is no spinner in NDJSON mode.
        if (cfg.outputFormat == OutputFormat.JSON) {
            cfg.noProgress = true;
        }

        // Fall back to VECTORBENCH_DATASET_DIR env var when no explicit CLI/config-file value was given.
        // This lets the Kubernetes StatefulSet set the dataset path once via env, without every kubectl
        // exec invocation having to pass --dataset-dir.
        if (!cmd.hasOption("dataset-dir") && cfg.datasetDir.equals("./datasets")) {
            String envDir = System.getenv("VECTORBENCH_DATASET_DIR");
            if (envDir != null && !envDir.isEmpty()) {
                cfg.datasetDir = envDir;
            }
        }

        return cfg;
    }

    private void applyProperties(Properties props) {
        if (props.containsKey("url")) {
            jdbcUrl = props.getProperty("url");
        }
        if (props.containsKey("protocol")) {
            protocol = parseProtocol(props.getProperty("protocol"));
        }
        if (props.containsKey("grpc-endpoint")) {
            grpcEndpoint = props.getProperty("grpc-endpoint");
        }
        if (props.containsKey("user")) {
            username = props.getProperty("user");
        }
        if (props.containsKey("password")) {
            password = props.getProperty("password");
        }
        if (props.containsKey("table")) {
            tableName = props.getProperty("table");
        }
        if (props.containsKey("dataset-dir")) {
            datasetDir = props.getProperty("dataset-dir");
        }
        if (props.containsKey("dataset")) {
            dataset = parseDataset(props.getProperty("dataset"));
        }
        if (props.containsKey("dataset-url")) {
            datasetUrl = props.getProperty("dataset-url");
        }
        if (props.containsKey("rows")) {
            numRows = Long.parseLong(props.getProperty("rows"));
        }
        if (props.containsKey("ingest-threads")) {
            ingestThreads = Integer.parseInt(props.getProperty("ingest-threads"));
        }
        if (props.containsKey("batch-size")) {
            batchSize = Integer.parseInt(props.getProperty("batch-size"));
        }
        if (props.containsKey("transaction-size")) {
            transactionSize = Integer.parseInt(props.getProperty("transaction-size"));
        }
        if (props.containsKey("query-threads")) {
            queryThreads = Integer.parseInt(props.getProperty("query-threads"));
        }
        if (props.containsKey("queries")) {
            queryCount = Integer.parseInt(props.getProperty("queries"));
        }
        if (props.containsKey("k")) {
            topK = Integer.parseInt(props.getProperty("k"));
        }
        if (props.containsKey("m")) {
            indexM = Integer.parseInt(props.getProperty("m"));
        }
        if (props.containsKey("beam-width")) {
            indexBeamWidth = Integer.parseInt(props.getProperty("beam-width"));
        }
        if (props.containsKey("index-num-shards")) {
            indexNumShards = Integer.parseInt(props.getProperty("index-num-shards"));
        }
        if (props.containsKey("neighbor-overflow")) {
            indexNeighborOverflow = Float.parseFloat(props.getProperty("neighbor-overflow"));
        }
        if (props.containsKey("alpha")) {
            indexAlpha = Float.parseFloat(props.getProperty("alpha"));
        }
        if (props.containsKey("skip-ingest")) {
            skipIngest = Boolean.parseBoolean(props.getProperty("skip-ingest"));
        }
        if (props.containsKey("skip-index")) {
            skipIndex = Boolean.parseBoolean(props.getProperty("skip-index"));
        }
        if (props.containsKey("skip-verify")) {
            skipVerify = Boolean.parseBoolean(props.getProperty("skip-verify"));
        }
        if (props.containsKey("skip-query")) {
            skipQuery = Boolean.parseBoolean(props.getProperty("skip-query"));
        }
        if (props.containsKey("drop-table")) {
            dropTable = Boolean.parseBoolean(props.getProperty("drop-table"));
        }
        if (props.containsKey("checkpoint")) {
            checkpoint = Boolean.parseBoolean(props.getProperty("checkpoint"));
        }
        if (props.containsKey("similarity")) {
            similarity = props.getProperty("similarity");
        }
        if (props.containsKey("client-timeout")) {
            clientTimeoutSeconds = Integer.parseInt(props.getProperty("client-timeout"));
        }
        if (props.containsKey("index-before-ingest")) {
            indexBeforeIngest = Boolean.parseBoolean(props.getProperty("index-before-ingest"));
        }
        if (props.containsKey("resume-from")) {
            parseResumeFrom(this, props.getProperty("resume-from"));
        }
        if (props.containsKey("ingest-max-ops")) {
            ingestMaxOpsPerSecond = Integer.parseInt(props.getProperty("ingest-max-ops"));
        }
        if (props.containsKey("query-max-ops")) {
            queryMaxOpsPerSecond = Integer.parseInt(props.getProperty("query-max-ops"));
        }
        if (props.containsKey("ingest-commit-retries")) {
            ingestCommitRetries = Integer.parseInt(props.getProperty("ingest-commit-retries"));
        }
        if (props.containsKey("checkpoint-timeout-seconds")) {
            checkpointTimeoutSeconds = Integer.parseInt(props.getProperty("checkpoint-timeout-seconds"));
        }
        if (props.containsKey("wait-for-indexes")) {
            waitForIndexes = Boolean.parseBoolean(props.getProperty("wait-for-indexes"));
        }
        if (props.containsKey("wait-for-indexes-timeout")) {
            waitForIndexesTimeoutSeconds = Integer.parseInt(props.getProperty("wait-for-indexes-timeout"));
        }
        if (props.containsKey("no-progress")) {
            noProgress = Boolean.parseBoolean(props.getProperty("no-progress"));
        }
        if (props.containsKey("output-format")) {
            outputFormat = parseOutputFormat(props.getProperty("output-format"));
        }
        if (props.containsKey("status-interval-seconds")) {
            statusIntervalSeconds = Integer.parseInt(props.getProperty("status-interval-seconds"));
        }
        if (props.containsKey("run-queries-during-ingestion")) {
            runQueriesDuringIngestion = Boolean.parseBoolean(props.getProperty("run-queries-during-ingestion"));
        }
        if (props.containsKey("run-queries-during-ingestion-period")) {
            runQueriesDuringIngestionPeriodSeconds = Integer.parseInt(
                    props.getProperty("run-queries-during-ingestion-period"));
        }
    }

    private static void parseResumeFrom(Config cfg, String raw) {
        if (raw != null && raw.trim().equalsIgnoreCase("auto")) {
            cfg.resumeFromAuto = true;
            cfg.resumeFrom = 0L;
        } else {
            cfg.resumeFromAuto = false;
            cfg.resumeFrom = Long.parseLong(raw);
        }
    }

    private static OutputFormat parseOutputFormat(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("output-format cannot be null");
        }
        return switch (raw.toLowerCase()) {
            case "text" -> OutputFormat.TEXT;
            case "json", "ndjson" -> OutputFormat.JSON;
            default -> throw new IllegalArgumentException("Unknown output-format: " + raw
                    + ". Supported: text, json");
        };
    }

    private static Protocol parseProtocol(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("protocol cannot be null");
        }
        return switch (raw.toLowerCase()) {
            case "jdbc" -> Protocol.JDBC;
            case "grpc" -> Protocol.GRPC;
            default -> throw new IllegalArgumentException("Unknown protocol: " + raw
                    + ". Supported: jdbc, grpc");
        };
    }

    private static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("on");
    }

    /**
     * Returns the effective transaction size: {@link #transactionSize} when set
     * to a positive value, otherwise {@link #batchSize}. The default of {@code 0}
     * preserves the legacy behaviour of one flush per commit.
     */
    public int effectiveTransactionSize() {
        int t = transactionSize;
        return t > 0 ? t : batchSize;
    }

    /**
     * Validates the joint invariants between {@link #batchSize},
     * {@link #transactionSize}, and {@link #ingestMaxOpsPerSecond}.
     *
     * <ul>
     *   <li>{@code batchSize >= 1}</li>
     *   <li>If {@code transactionSize > 0}, then {@code transactionSize >= batchSize}</li>
     *   <li>If {@code ingestMaxOpsPerSecond > 0}, then
     *       {@code effectiveTransactionSize() <= ingestMaxOpsPerSecond}.
     *       The rate limiter is acquired per commit, so the safety bound applies
     *       to the commit unit (not the per-flush batch).</li>
     * </ul>
     *
     * @throws ParseException if any invariant is violated, with a message that
     *         names the offending values.
     */
    static void validateBatchAndTransactionInvariants(Config cfg) throws ParseException {
        if (cfg.batchSize <= 0) {
            throw new ParseException("--batch-size must be >= 1, got " + cfg.batchSize);
        }
        if (cfg.transactionSize < 0) {
            throw new ParseException("--transaction-size must be >= 0, got " + cfg.transactionSize);
        }
        if (cfg.transactionSize > 0 && cfg.transactionSize < cfg.batchSize) {
            throw new ParseException("--transaction-size (" + cfg.transactionSize
                    + ") must be >= --batch-size (" + cfg.batchSize + ")");
        }
        int effectiveTxn = cfg.effectiveTransactionSize();
        if (cfg.ingestMaxOpsPerSecond > 0 && effectiveTxn > cfg.ingestMaxOpsPerSecond) {
            throw new ParseException("effective transaction size (" + effectiveTxn
                    + ") must be <= --ingest-max-ops (" + cfg.ingestMaxOpsPerSecond
                    + "). Either lower --batch-size/--transaction-size or raise --ingest-max-ops "
                    + "(or set --ingest-max-ops 0 for unlimited).");
        }
    }

    /**
     * Validates the jvector index-build float parameters.
     *
     * <ul>
     *   <li>{@code neighborOverflow} must be finite and {@code > 0}</li>
     *   <li>{@code alpha} must be finite and {@code > 0}</li>
     * </ul>
     *
     * @throws ParseException if any invariant is violated, with a message that
     *         names the offending parameter and its value.
     */
    static void validateIndexBuildParams(Config cfg) throws ParseException {
        if (!Float.isFinite(cfg.indexNeighborOverflow) || cfg.indexNeighborOverflow <= 0) {
            throw new ParseException("--neighbor-overflow must be a finite positive number, got "
                    + cfg.indexNeighborOverflow);
        }
        if (!Float.isFinite(cfg.indexAlpha) || cfg.indexAlpha <= 0) {
            throw new ParseException("--alpha must be a finite positive number, got "
                    + cfg.indexAlpha);
        }
    }

    /** Returns the similarity function: CLI override if set, otherwise dataset default. */
    String effectiveSimilarity() {
        return similarity != null ? similarity : dataset.similarity;
    }

    /** Returns the JDBC URL with client.timeout embedded as a query parameter. */
    String effectiveJdbcUrl() {
        long timeoutMs = (long) clientTimeoutSeconds * 1000;
        String sep = jdbcUrl.contains("?") ? "&" : "?";
        return jdbcUrl + sep + "client.timeout=" + timeoutMs;
    }

    private static DatasetLoader.DatasetPreset parseDataset(String value) {
        return switch (value.toLowerCase()) {
            case "sift10k", "siftsmall" -> DatasetLoader.DatasetPreset.SIFT10K;
            case "sift1m", "sift" -> DatasetLoader.DatasetPreset.SIFT1M;
            case "gist1m", "gist" -> DatasetLoader.DatasetPreset.GIST1M;
            case "sift10m" -> DatasetLoader.DatasetPreset.SIFT10M;
            case "bigann", "sift1b" -> DatasetLoader.DatasetPreset.BIGANN;
            case "glove100", "glove-100", "glove" -> DatasetLoader.DatasetPreset.GLOVE_100;
            case "deep-image-96", "deep-image", "deepimage" -> DatasetLoader.DatasetPreset.DEEP_IMAGE_96;
            case "custom" -> DatasetLoader.DatasetPreset.CUSTOM;
            default -> throw new IllegalArgumentException("Unknown dataset: " + value
                    + ". Supported: sift10k, sift1m, gist1m, sift10m, bigann, glove100, deep-image-96, custom");
        };
    }

    @Override
    public String toString() {
        return "Config{"
                + "jdbcUrl='" + jdbcUrl + '\''
                + ", dataset=" + dataset.name()
                + ", table='" + tableName + '\''
                + ", rows=" + numRows
                + ", ingestThreads=" + ingestThreads
                + ", batchSize=" + batchSize
                + ", transactionSize=" + effectiveTransactionSize()
                + ", queryThreads=" + queryThreads
                + ", queries=" + queryCount
                + ", topK=" + topK
                + ", indexM=" + indexM
                + ", beamWidth=" + indexBeamWidth
                + ", neighborOverflow=" + indexNeighborOverflow
                + ", alpha=" + indexAlpha
                + ", similarity=" + effectiveSimilarity()
                + (similarity != null ? " (override)" : " (dataset default)")
                + (resumeFrom > 0 ? ", resumeFrom=" + resumeFrom : "")
                + ", ingestMaxOpsPerSecond=" + (ingestMaxOpsPerSecond > 0 ? ingestMaxOpsPerSecond : "unlimited")
                + ", queryMaxOpsPerSecond=" + (queryMaxOpsPerSecond > 0 ? queryMaxOpsPerSecond : "unlimited")
                + ", ingestCommitRetries=" + ingestCommitRetries
                + ", indexBeforeIngest=" + indexBeforeIngest
                + ", skipIngest=" + skipIngest
                + ", skipIndex=" + skipIndex
                + ", skipVerify=" + skipVerify
                + ", skipQuery=" + skipQuery
                + ", dropTable=" + dropTable
                + ", checkpoint=" + checkpoint
                + ", checkpointTimeoutSeconds=" + checkpointTimeoutSeconds
                + ", waitForIndexes=" + waitForIndexes
                + ", waitForIndexesTimeoutSeconds=" + waitForIndexesTimeoutSeconds
                + ", clientTimeoutSeconds=" + clientTimeoutSeconds
                + ", noProgress=" + noProgress
                + ", outputFormat=" + outputFormat
                + ", statusIntervalSeconds=" + statusIntervalSeconds
                + ", runQueriesDuringIngestion=" + runQueriesDuringIngestion
                + ", runQueriesDuringIngestionPeriodSeconds=" + runQueriesDuringIngestionPeriodSeconds
                + '}';
    }
}
