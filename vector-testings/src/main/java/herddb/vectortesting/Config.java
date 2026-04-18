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

    String jdbcUrl = "jdbc:herddb:server:localhost:7000";
    String username = "sa";
    String password = "hdb";
    String tableName = "vector_bench";
    String datasetDir = "./datasets";
    String datasetUrl = null; // null means use preset default
    DatasetLoader.DatasetPreset dataset = DatasetLoader.DatasetPreset.SIFT1M;
    int numRows = 100_000;
    int ingestThreads = 4;
    int batchSize = 500;
    int queryThreads = 4;
    int queryCount = 1000;
    int topK = 10;
    boolean topKExplicit = false;
    int indexM = 16;
    int indexBeamWidth = 100;
    boolean skipIngest = false;
    boolean skipIndex = false;
    boolean skipVerify = false;
    boolean dropTable = false;
    boolean checkpoint = false;
    int clientTimeoutSeconds = 7200 * 4; // 8 hours
    String similarity = null; // null = use dataset default
    boolean indexBeforeIngest = true;
    int resumeFrom = 0; // skip first N vectors; row IDs start from N
    int ingestMaxOpsPerSecond = 100_000; // 0 = unlimited
    int ingestCommitRetries = 3;
    int checkpointTimeoutSeconds = 300;
    boolean noProgress = false;
    OutputFormat outputFormat = OutputFormat.TEXT;
    /**
     * Interval in seconds between periodic {@code [status]} dumps during ingestion.
     * A dedicated JDBC connection queries {@code syslogstatus}, {@code systablestats} and
     * {@code sysindexstatus} every {@code N} seconds so checkpoint / index-tail lag is
     * visible in the run log. {@code 0} disables the status thread entirely.
     */
    int statusIntervalSeconds = 60;

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
        opts.addOption(null, "batch-size", true, "Commit after N inserts (default: 500)");
        opts.addOption(null, "query-threads", true, "Query parallelism (default: 4)");
        opts.addOption(null, "queries", true, "Number of ANN queries to execute (default: 1000)");
        opts.addOption("k", null, true, "LIMIT K for ANN queries (default: 10)");
        opts.addOption(null, "m", true, "Vector index M parameter (default: 16)");
        opts.addOption(null, "beam-width", true, "Vector index beamWidth (default: 100)");
        opts.addOption(null, "skip-ingest", false, "Skip ingestion phase");
        opts.addOption(null, "skip-index", false, "Skip index creation");
        opts.addOption(null, "skip-verify", false, "Skip row count verification after ingestion");
        opts.addOption(null, "drop-table", false, "Drop table before starting");
        opts.addOption(null, "checkpoint", false, "Force checkpoint after ingestion and after index creation");
        opts.addOption(null, "similarity", true, "Similarity function: euclidean, cosine, dot (default: from dataset)");
        opts.addOption(null, "client-timeout", true, "Client request timeout in seconds (default: 7200)");
        opts.addOption(null, "index-before-ingest", false, "Create vector index before ingestion instead of after");
        opts.addOption(null, "resume-from", true, "Skip first N vectors and start row IDs from N (default: 0)");
        opts.addOption(null, "ingest-max-ops", true, "Max ingestion ops/s across all threads, 0=unlimited (default: 100000)");
        opts.addOption(null, "ingest-commit-retries", true,
                "Retries per failed batch commit before failing the run (default: 3, "
                        + "exponential back-off 10s/20s/40s...)");
        opts.addOption(null, "checkpoint-timeout-seconds", true, "Seconds to wait for the Indexing Service to catch up during --checkpoint (default: 300)");
        opts.addOption(null, "no-progress", false,
                "Disable animated spinner; emit one plain \\n-terminated line per progress sample "
                        + "(implicitly enabled when VECTOR_BENCH_NO_PROGRESS=1 or --output-format=json)");
        opts.addOption(null, "output-format", true,
                "Output format: text (default) or json (NDJSON, one object per line). json implies --no-progress.");
        opts.addOption(null, "status-interval-seconds", true,
                "Seconds between server-status dumps during ingestion; 0 disables (default: 60)");
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
            cfg.numRows = Integer.parseInt(cmd.getOptionValue("rows"));
        }
        if (cmd.hasOption("ingest-threads")) {
            cfg.ingestThreads = Integer.parseInt(cmd.getOptionValue("ingest-threads"));
        }
        if (cmd.hasOption("batch-size")) {
            cfg.batchSize = Integer.parseInt(cmd.getOptionValue("batch-size"));
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
        if (cmd.hasOption("skip-ingest")) {
            cfg.skipIngest = true;
        }
        if (cmd.hasOption("skip-index")) {
            cfg.skipIndex = true;
        }
        if (cmd.hasOption("skip-verify")) {
            cfg.skipVerify = true;
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
            cfg.resumeFrom = Integer.parseInt(cmd.getOptionValue("resume-from"));
        }
        if (cmd.hasOption("ingest-max-ops")) {
            cfg.ingestMaxOpsPerSecond = Integer.parseInt(cmd.getOptionValue("ingest-max-ops"));
        }
        if (cmd.hasOption("ingest-commit-retries")) {
            cfg.ingestCommitRetries = Integer.parseInt(cmd.getOptionValue("ingest-commit-retries"));
        }
        if (cmd.hasOption("checkpoint-timeout-seconds")) {
            cfg.checkpointTimeoutSeconds = Integer.parseInt(cmd.getOptionValue("checkpoint-timeout-seconds"));
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
            numRows = Integer.parseInt(props.getProperty("rows"));
        }
        if (props.containsKey("ingest-threads")) {
            ingestThreads = Integer.parseInt(props.getProperty("ingest-threads"));
        }
        if (props.containsKey("batch-size")) {
            batchSize = Integer.parseInt(props.getProperty("batch-size"));
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
        if (props.containsKey("skip-ingest")) {
            skipIngest = Boolean.parseBoolean(props.getProperty("skip-ingest"));
        }
        if (props.containsKey("skip-index")) {
            skipIndex = Boolean.parseBoolean(props.getProperty("skip-index"));
        }
        if (props.containsKey("skip-verify")) {
            skipVerify = Boolean.parseBoolean(props.getProperty("skip-verify"));
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
            resumeFrom = Integer.parseInt(props.getProperty("resume-from"));
        }
        if (props.containsKey("ingest-max-ops")) {
            ingestMaxOpsPerSecond = Integer.parseInt(props.getProperty("ingest-max-ops"));
        }
        if (props.containsKey("ingest-commit-retries")) {
            ingestCommitRetries = Integer.parseInt(props.getProperty("ingest-commit-retries"));
        }
        if (props.containsKey("checkpoint-timeout-seconds")) {
            checkpointTimeoutSeconds = Integer.parseInt(props.getProperty("checkpoint-timeout-seconds"));
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

    private static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("on");
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
                + ", queryThreads=" + queryThreads
                + ", queries=" + queryCount
                + ", topK=" + topK
                + ", indexM=" + indexM
                + ", beamWidth=" + indexBeamWidth
                + ", similarity=" + effectiveSimilarity()
                + (similarity != null ? " (override)" : " (dataset default)")
                + (resumeFrom > 0 ? ", resumeFrom=" + resumeFrom : "")
                + ", ingestMaxOpsPerSecond=" + (ingestMaxOpsPerSecond > 0 ? ingestMaxOpsPerSecond : "unlimited")
                + ", ingestCommitRetries=" + ingestCommitRetries
                + ", indexBeforeIngest=" + indexBeforeIngest
                + ", skipIngest=" + skipIngest
                + ", skipIndex=" + skipIndex
                + ", skipVerify=" + skipVerify
                + ", dropTable=" + dropTable
                + ", checkpoint=" + checkpoint
                + ", checkpointTimeoutSeconds=" + checkpointTimeoutSeconds
                + ", clientTimeoutSeconds=" + clientTimeoutSeconds
                + ", noProgress=" + noProgress
                + ", outputFormat=" + outputFormat
                + ", statusIntervalSeconds=" + statusIntervalSeconds
                + '}';
    }
}
