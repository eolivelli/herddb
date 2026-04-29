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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Read-only CLI tool that prints the contents of a dataset descriptor JSON. Designed to
 * be invoked by automation (bench-driving agents) that need to discover what sizes and
 * similarity a dataset supports without spinning up the full {@code VectorBench} JVM.
 *
 * <p>Accepts a local file path or a remote URL ({@code http://}, {@code https://},
 * {@code ftp://}, {@code gs://}). Remote URLs are downloaded to a temp file via
 * {@link DatasetLoader#downloadSmartUrl(String, java.io.File)} so credentials and
 * GCS routing work the same as for the bench loader.</p>
 */
public final class DatasetDescribe {

    private DatasetDescribe() {
        // entry point only
    }

    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption(null, "descriptor", true,
                "Path or URL of the dataset descriptor JSON (required). Supported schemes:"
                        + " file:// (or local path), http(s)://, ftp://, gs://.");
        opts.addOption("h", "help", false, "Show help");

        CommandLine cmd;
        try {
            cmd = new DefaultParser().parse(opts, args);
        } catch (ParseException e) {
            System.err.println("ERROR: " + e.getMessage());
            new HelpFormatter().printHelp("dataset-describe", opts);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help") || !cmd.hasOption("descriptor")) {
            new HelpFormatter().printHelp("dataset-describe", opts);
            System.exit(cmd.hasOption("help") ? 0 : 1);
            return;
        }

        String descriptorArg = cmd.getOptionValue("descriptor");
        try {
            File descriptorFile = resolveDescriptor(descriptorArg);
            DatasetLoader.DatasetDescriptor desc = DatasetLoader.DatasetDescriptor.load(descriptorFile);
            print(System.out, desc);
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Returns a local {@link File} for the descriptor argument. Local paths are returned
     * verbatim; URLs are downloaded to a temp file (deleted on JVM exit) using the same
     * smart-URL logic as the dataset loader.
     */
    static File resolveDescriptor(String arg) throws IOException {
        if (arg.startsWith("http://") || arg.startsWith("https://")
                || arg.startsWith("ftp://") || arg.startsWith("gs://")) {
            File tmp = Files.createTempFile("descriptor-", ".json").toFile();
            tmp.deleteOnExit();
            // Construct a throwaway DatasetLoader to reuse its URL-resolution code path.
            // The constructor arguments don't matter for downloadSmartUrl (it doesn't
            // touch the dataset directory). Using CUSTOM with a null URL is the
            // lightest-weight option.
            DatasetLoader loader = new DatasetLoader("/tmp", DatasetLoader.DatasetPreset.CUSTOM, null);
            loader.downloadSmartUrl(arg, tmp);
            return tmp;
        }
        if (arg.startsWith("file://")) {
            return new File(arg.substring("file://".length()));
        }
        return new File(arg);
    }

    /**
     * Pretty-prints the descriptor as fixed-width key/value lines, easy for agents
     * to grep. Followed by a {@code groundTruthCheckpoints} block listing each
     * (count, file) pair on its own indented line.
     */
    static void print(PrintStream out, DatasetLoader.DatasetDescriptor desc) {
        out.printf("%-22s %s%n", "name", desc.name);
        out.printf("%-22s %d%n", "dimensions", desc.dimensions);
        out.printf("%-22s %s%n", "similarity", desc.similarity);
        out.printf("%-22s %s%n", "format", desc.format.name().toLowerCase(java.util.Locale.ROOT));
        out.printf("%-22s %d%n", "totalVectors", desc.totalVectors);
        out.printf("%-22s %d%n", "numQueries", desc.numQueries);
        out.printf("%-22s %d%n", "groundTruthK", desc.groundTruthK);
        out.printf("%-22s %s%n", "baseFile", desc.baseFile == null ? "" : desc.baseFile);
        out.printf("%-22s %s%n", "queryFile", desc.queryFile == null ? "" : desc.queryFile);
        out.printf("%-22s %s%n", "groundTruthFile",
                desc.groundTruthFile == null ? "" : desc.groundTruthFile);
        out.printf("%-22s %d%n", "groundTruthCheckpoints", desc.groundTruthCheckpoints.size());
        for (DatasetLoader.DatasetDescriptor.GroundTruthCheckpoint cp : desc.groundTruthCheckpoints) {
            out.printf("  %-20d %s%n", cp.baseVectorCount, cp.file);
        }
    }
}
