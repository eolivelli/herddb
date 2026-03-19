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

    String jdbcUrl = "jdbc:herddb:server:localhost:7000";
    String username = "sa";
    String password = "";
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
    int indexM = 16;
    int indexBeamWidth = 100;
    boolean skipIngest = false;
    boolean skipIndex = false;
    boolean skipVerify = false;
    boolean dropTable = false;

    private static Options buildOptions() {
        Options opts = new Options();
        opts.addOption("u", "url", true, "JDBC URL (default: jdbc:herddb:server:localhost:7000)");
        opts.addOption(null, "user", true, "Username (default: sa)");
        opts.addOption(null, "password", true, "Password (default: empty)");
        opts.addOption(null, "table", true, "Table name (default: vector_bench)");
        opts.addOption(null, "dataset-dir", true, "Dataset download/cache directory (default: ./datasets)");
        opts.addOption(null, "dataset", true, "Dataset preset: sift1m, sift10m, bigann (default: sift1m)");
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
        if (cmd.hasOption("url")) cfg.jdbcUrl = cmd.getOptionValue("url");
        if (cmd.hasOption("user")) cfg.username = cmd.getOptionValue("user");
        if (cmd.hasOption("password")) cfg.password = cmd.getOptionValue("password");
        if (cmd.hasOption("table")) cfg.tableName = cmd.getOptionValue("table");
        if (cmd.hasOption("dataset-dir")) cfg.datasetDir = cmd.getOptionValue("dataset-dir");
        if (cmd.hasOption("dataset")) cfg.dataset = parseDataset(cmd.getOptionValue("dataset"));
        if (cmd.hasOption("dataset-url")) cfg.datasetUrl = cmd.getOptionValue("dataset-url");
        if (cmd.hasOption("rows")) cfg.numRows = Integer.parseInt(cmd.getOptionValue("rows"));
        if (cmd.hasOption("ingest-threads")) cfg.ingestThreads = Integer.parseInt(cmd.getOptionValue("ingest-threads"));
        if (cmd.hasOption("batch-size")) cfg.batchSize = Integer.parseInt(cmd.getOptionValue("batch-size"));
        if (cmd.hasOption("query-threads")) cfg.queryThreads = Integer.parseInt(cmd.getOptionValue("query-threads"));
        if (cmd.hasOption("queries")) cfg.queryCount = Integer.parseInt(cmd.getOptionValue("queries"));
        if (cmd.hasOption("k")) cfg.topK = Integer.parseInt(cmd.getOptionValue("k"));
        if (cmd.hasOption("m")) cfg.indexM = Integer.parseInt(cmd.getOptionValue("m"));
        if (cmd.hasOption("beam-width")) cfg.indexBeamWidth = Integer.parseInt(cmd.getOptionValue("beam-width"));
        if (cmd.hasOption("skip-ingest")) cfg.skipIngest = true;
        if (cmd.hasOption("skip-index")) cfg.skipIndex = true;
        if (cmd.hasOption("skip-verify")) cfg.skipVerify = true;
        if (cmd.hasOption("drop-table")) cfg.dropTable = true;

        return cfg;
    }

    private void applyProperties(Properties props) {
        if (props.containsKey("url")) jdbcUrl = props.getProperty("url");
        if (props.containsKey("user")) username = props.getProperty("user");
        if (props.containsKey("password")) password = props.getProperty("password");
        if (props.containsKey("table")) tableName = props.getProperty("table");
        if (props.containsKey("dataset-dir")) datasetDir = props.getProperty("dataset-dir");
        if (props.containsKey("dataset")) dataset = parseDataset(props.getProperty("dataset"));
        if (props.containsKey("dataset-url")) datasetUrl = props.getProperty("dataset-url");
        if (props.containsKey("rows")) numRows = Integer.parseInt(props.getProperty("rows"));
        if (props.containsKey("ingest-threads")) ingestThreads = Integer.parseInt(props.getProperty("ingest-threads"));
        if (props.containsKey("batch-size")) batchSize = Integer.parseInt(props.getProperty("batch-size"));
        if (props.containsKey("query-threads")) queryThreads = Integer.parseInt(props.getProperty("query-threads"));
        if (props.containsKey("queries")) queryCount = Integer.parseInt(props.getProperty("queries"));
        if (props.containsKey("k")) topK = Integer.parseInt(props.getProperty("k"));
        if (props.containsKey("m")) indexM = Integer.parseInt(props.getProperty("m"));
        if (props.containsKey("beam-width")) indexBeamWidth = Integer.parseInt(props.getProperty("beam-width"));
        if (props.containsKey("skip-ingest")) skipIngest = Boolean.parseBoolean(props.getProperty("skip-ingest"));
        if (props.containsKey("skip-index")) skipIndex = Boolean.parseBoolean(props.getProperty("skip-index"));
        if (props.containsKey("skip-verify")) skipVerify = Boolean.parseBoolean(props.getProperty("skip-verify"));
        if (props.containsKey("drop-table")) dropTable = Boolean.parseBoolean(props.getProperty("drop-table"));
    }

    private static DatasetLoader.DatasetPreset parseDataset(String value) {
        return switch (value.toLowerCase()) {
            case "sift1m", "sift" -> DatasetLoader.DatasetPreset.SIFT1M;
            case "sift10m" -> DatasetLoader.DatasetPreset.SIFT10M;
            case "bigann", "sift1b" -> DatasetLoader.DatasetPreset.BIGANN;
            default -> throw new IllegalArgumentException("Unknown dataset: " + value
                    + ". Supported: sift1m, sift10m, bigann");
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
                + ", skipIngest=" + skipIngest
                + ", skipIndex=" + skipIndex
                + ", skipVerify=" + skipVerify
                + ", dropTable=" + dropTable
                + '}';
    }
}
