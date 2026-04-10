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
    int threads = 2;

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
        opts.addOption(null, "threads", true, "Parallel Ollama embedding workers (default: 2)");
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
        if (cmd.hasOption("threads")) {
            cfg.threads = Integer.parseInt(cmd.getOptionValue("threads"));
        }

        if (cfg.threads <= 0) {
            System.err.println("ERROR: --threads must be positive.");
            System.exit(1);
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

        return cfg;
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
                + ", threads=" + threads
                + '}';
    }
}
