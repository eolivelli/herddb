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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class GeneratorConfig {

    int total = -1;
    String name = null;
    String outputDir = "./datasets/generated";
    String model = "all-minilm";
    String ollamaUrl = "http://localhost:11434";
    int numQueries = 1000;
    int groundTruthK = 100;
    boolean csv = false;
    boolean zip = false;
    int batchSize = 100;
    String similarity = "euclidean";
    /**
     * Ascending list of base-vector counts at which a ground-truth IVECS
     * file must be emitted. The final entry always equals {@link #total}
     * (so the legacy single ground-truth file is always produced). When
     * the user does not pass {@code --ground-truth-checkpoints}, this is
     * a singleton {@code [total]}.
     */
    long[] groundTruthCheckpoints = null;

    private static Options buildOptions() {
        Options opts = new Options();
        opts.addOption(null, "total", true, "Total number of sentences to generate (required)");
        opts.addOption(null, "name", true, "Dataset name (used in descriptor and file naming)");
        opts.addOption(null, "output-dir", true, "Output directory (default: ./datasets/generated)");
        opts.addOption(null, "model", true, "Ollama embedding model (default: all-minilm)");
        opts.addOption(null, "ollama-url", true, "Ollama server URL (default: http://localhost:11434)");
        opts.addOption(null, "num-queries", true, "Number of query vectors from first N sentences (default: 1000)");
        opts.addOption(null, "ground-truth-k", true, "K nearest neighbors per query for ground truth (default: 100)");
        opts.addOption(null, "csv", false, "Also generate CSV file with id, sentence, vector");
        opts.addOption(null, "zip", false, "Compress output files into a ZIP archive");
        opts.addOption(null, "batch-size", true, "Sentences per Ollama API call (default: 100)");
        opts.addOption(null, "similarity", true, "Distance function: euclidean, cosine (default: euclidean)");
        opts.addOption(null, "ground-truth-checkpoints", true,
                "Comma-separated ascending base-vector counts at which intermediate ground-truth"
                        + " IVECS files are emitted (e.g. 1000000,10000000,50000000). The final ground"
                        + " truth at --total is always written; checkpoint counts must be strictly"
                        + " greater than --num-queries and not exceed --total.");
        opts.addOption("h", "help", false, "Show help");
        return opts;
    }

    static GeneratorConfig parse(String[] args) throws ParseException {
        Options opts = buildOptions();
        CommandLine cmd = new DefaultParser().parse(opts, args);

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("dataset-generator", opts);
            System.exit(0);
        }

        GeneratorConfig cfg = new GeneratorConfig();

        if (cmd.hasOption("total")) {
            cfg.total = Integer.parseInt(cmd.getOptionValue("total"));
        }
        if (cmd.hasOption("name")) {
            cfg.name = cmd.getOptionValue("name");
        }
        if (cmd.hasOption("output-dir")) {
            cfg.outputDir = cmd.getOptionValue("output-dir");
        }
        if (cmd.hasOption("model")) {
            cfg.model = cmd.getOptionValue("model");
        }
        if (cmd.hasOption("ollama-url")) {
            cfg.ollamaUrl = cmd.getOptionValue("ollama-url");
        }
        if (cmd.hasOption("num-queries")) {
            cfg.numQueries = Integer.parseInt(cmd.getOptionValue("num-queries"));
        }
        if (cmd.hasOption("ground-truth-k")) {
            cfg.groundTruthK = Integer.parseInt(cmd.getOptionValue("ground-truth-k"));
        }
        if (cmd.hasOption("csv")) {
            cfg.csv = true;
        }
        if (cmd.hasOption("zip")) {
            cfg.zip = true;
        }
        if (cmd.hasOption("batch-size")) {
            cfg.batchSize = Integer.parseInt(cmd.getOptionValue("batch-size"));
        }
        if (cmd.hasOption("similarity")) {
            cfg.similarity = cmd.getOptionValue("similarity");
        }

        if (cfg.total <= 0) {
            System.err.println("ERROR: --total is required and must be positive.");
            new HelpFormatter().printHelp("dataset-generator", opts);
            System.exit(1);
        }
        if (cfg.numQueries > cfg.total) {
            System.err.println("ERROR: --num-queries (" + cfg.numQueries
                    + ") must be <= --total (" + cfg.total + ").");
            System.exit(1);
        }

        cfg.groundTruthCheckpoints = parseCheckpoints(
                cmd.getOptionValue("ground-truth-checkpoints"), cfg.total, cfg.numQueries);

        return cfg;
    }

    /**
     * Parse the {@code --ground-truth-checkpoints} CSV value into a sorted, deduped,
     * ascending {@code long[]} that always ends with {@code total}.
     *
     * <p>Validation rules:</p>
     * <ul>
     *   <li>Each entry must parse as a positive long.</li>
     *   <li>Each entry must be {@code > numQueries} (a checkpoint at or below the query
     *       count would yield meaningless ground truth, since the tracker is only
     *       initialised after the first {@code numQueries} vectors).</li>
     *   <li>Each entry must be {@code <= total}.</li>
     *   <li>{@code total} is always appended (deduped) so the legacy single ground
     *       truth file is produced.</li>
     * </ul>
     *
     * <p>When {@code csv} is {@code null} or blank, returns a singleton {@code [total]}
     * — i.e. the historical single-ground-truth behaviour.</p>
     *
     * @throws IllegalArgumentException for any validation failure (unparseable entry,
     *     duplicate, out-of-range value).
     */
    static long[] parseCheckpoints(String csv, int total, int numQueries) {
        Set<Long> sorted = new TreeSet<>();
        if (csv != null && !csv.trim().isEmpty()) {
            // Use LinkedHashSet first to detect duplicates (a duplicate checkpoint count
            // is almost certainly a typo and silently swallowing it would be confusing).
            Set<Long> seen = new LinkedHashSet<>();
            for (String token : csv.split(",")) {
                String t = token.trim();
                if (t.isEmpty()) {
                    continue;
                }
                long v;
                try {
                    v = Long.parseLong(t);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid --ground-truth-checkpoints entry: '" + t + "' is not a number");
                }
                if (v <= numQueries) {
                    throw new IllegalArgumentException("Ground-truth checkpoint " + v
                            + " must be greater than --num-queries (" + numQueries + ")");
                }
                if (v > total) {
                    throw new IllegalArgumentException("Ground-truth checkpoint " + v
                            + " exceeds --total (" + total + ")");
                }
                if (!seen.add(v)) {
                    throw new IllegalArgumentException(
                            "Duplicate --ground-truth-checkpoints entry: " + v);
                }
                sorted.add(v);
            }
        }
        // The total checkpoint is always emitted; deduped automatically by TreeSet.
        sorted.add((long) total);
        long[] out = new long[sorted.size()];
        int i = 0;
        for (long v : sorted) {
            out[i++] = v;
        }
        return out;
    }

    @Override
    public String toString() {
        return "GeneratorConfig{"
                + "total=" + total
                + ", name='" + name + '\''
                + ", outputDir='" + outputDir + '\''
                + ", model='" + model + '\''
                + ", ollamaUrl='" + ollamaUrl + '\''
                + ", numQueries=" + numQueries
                + ", groundTruthK=" + groundTruthK
                + ", csv=" + csv
                + ", zip=" + zip
                + ", batchSize=" + batchSize
                + ", similarity=" + similarity
                + ", groundTruthCheckpoints=" + Arrays.toString(groundTruthCheckpoints)
                + '}';
    }
}
