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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import herddb.vectortesting.DatasetLoader.DatasetPreset;
import herddb.vectortesting.DatasetLoader.VecFormat;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Prepares a standard benchmark dataset (BIGANN or GIST1M) for consumption in
 * VectorBench "custom" mode: stages a flat directory containing the native
 * data files plus a generated {@code *_descriptor.json}, and optionally
 * invokes {@code push_dataset_gcs.sh} to upload everything to a Google Cloud
 * Storage path.
 *
 * <p>The goal is to publish the standard datasets (normally fetched via FTP
 * from the TEXMEX mirror) into a project-owned bucket
 * ({@code gs://herddb-datasets}) so that cloud runs no longer depend on an
 * external FTP server.
 */
public class StandardDatasetPublisher {

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) throws Exception {
        PublisherConfig config = PublisherConfig.parse(args);
        System.out.println("Standard Dataset Publisher");
        System.out.println("  Dataset:     " + config.datasetName);
        System.out.println("  Source dir:  " + config.datasetDir);
        System.out.println("  Output dir:  " + config.outputDir);
        System.out.println("  GCS path:    " + (config.gsPath != null ? config.gsPath : "(upload skipped)"));

        // Ensure source files exist (downloading from FTP if needed). Skip the
        // query/gt ensure call when the files are already on disk under any
        // known layout — ensureBigannFile blindly re-invokes `tar` otherwise
        // and the second extraction fails because files already exist.
        DatasetLoader loader = new DatasetLoader(config.datasetDir, config.preset, null);
        loader.ensureDataset();
        File sourceDir = loader.getDatasetSubDir();
        if (!queryAndGroundTruthAlreadyPresent(sourceDir, config.preset)) {
            loader.ensureQueryAndGroundTruth();
        } else {
            System.out.println("Query and ground-truth files already present, skipping download.");
        }

        File outputDir = new File(config.outputDir);
        if (!outputDir.mkdirs() && !outputDir.isDirectory()) {
            throw new IOException("Cannot create output directory: " + outputDir.getAbsolutePath());
        }

        // Stage the three flat data files under canonical names.
        File stagedBase = stageFile(sourceDir, config.preset.baseFile, outputDir, config.preset.baseFile);
        File stagedQuery = stageFile(sourceDir, config.preset.queryFile, outputDir, config.preset.queryFile);
        // BIGANN's ground truth lives under a subdirectory and — in the
        // real TEXMEX tarball — under a different filename convention than
        // the preset declares; resolve whichever the local copy actually has.
        String sourceGtName = resolveGroundTruthSource(sourceDir, config.preset);
        String stagedGtName = flattenGroundTruthName(config.preset);
        File stagedGt = stageFile(sourceDir, sourceGtName, outputDir, stagedGtName);

        // Inspect the staged files to derive metadata for the descriptor.
        VecFormat baseFormat = config.preset.baseFormat;
        int dimensions = readFirstInt(stagedBase);
        long recordBytes = recordBytes(baseFormat, dimensions);
        long totalVectors = stagedBase.length() / recordBytes;
        long recordBytesQuery = recordBytes(config.preset.queryFormat, dimensions);
        long numQueries = stagedQuery.length() / recordBytesQuery;
        int groundTruthK = readFirstInt(stagedGt);
        long gtRecordBytes = 4L + 4L * groundTruthK;
        long gtRecords = stagedGt.length() / gtRecordBytes;

        System.out.println("Derived metadata:");
        System.out.println("  format:        " + baseFormat.name().toLowerCase(Locale.ROOT));
        System.out.println("  dimensions:    " + dimensions);
        System.out.println("  totalVectors:  " + totalVectors);
        System.out.println("  numQueries:    " + numQueries + " (ground-truth records: " + gtRecords + ")");
        System.out.println("  groundTruthK:  " + groundTruthK);

        if (gtRecords != numQueries) {
            System.out.println("WARNING: ground-truth record count (" + gtRecords
                    + ") does not match numQueries (" + numQueries + "); check the source files.");
        }

        // Write descriptor JSON.
        File descriptorFile = new File(outputDir, config.datasetName + "_descriptor.json");
        ObjectNode root = JSON.createObjectNode();
        root.put("name", config.datasetName);
        root.put("format", baseFormat.name().toLowerCase(Locale.ROOT));
        root.put("dimensions", dimensions);
        root.put("similarity", config.preset.similarity);
        root.put("totalVectors", totalVectors);
        root.put("numQueries", numQueries);
        root.put("groundTruthK", groundTruthK);
        root.put("baseFile", stagedBase.getName());
        root.put("queryFile", stagedQuery.getName());
        root.put("groundTruthFile", stagedGt.getName());
        root.put("source", config.preset.defaultUrl);
        root.put("createdAt", Instant.now().toString());
        JSON.writeValue(descriptorFile, root);
        System.out.println("Descriptor written: " + descriptorFile.getAbsolutePath());

        if (config.gsPath != null) {
            runPushScript(outputDir, config.gsPath);
        } else {
            System.out.println();
            System.out.println("Staging complete. To upload, run:");
            System.out.println("  ./push_dataset_gcs.sh " + outputDir.getAbsolutePath()
                    + " gs://herddb-datasets/" + config.datasetName + "/");
        }
    }

    /** Returns the flat ground-truth filename for a given preset. */
    private static String flattenGroundTruthName(DatasetPreset preset) {
        // Presets like BIGANN have a nested ground-truth path
        // (e.g. "bigann_gnd/idx_1000000000.ivecs"); custom mode expects a flat
        // file layout next to the descriptor.
        if (preset == DatasetPreset.BIGANN) {
            return "bigann_groundtruth.ivecs";
        }
        if (preset == DatasetPreset.SIFT10M) {
            return "bigann_groundtruth.ivecs";
        }
        return preset.groundTruthFile;
    }

    /**
     * Return the relative path under {@code sourceDir} where the ground truth
     * for {@code preset} actually lives on disk. The BIGANN preset declares
     * {@code bigann_gnd/idx_1000000000.ivecs} but the real
     * {@code bigann_gnd.tar.gz} from TEXMEX extracts to
     * {@code gnd/idx_1000M.ivecs}; try both so a freshly-downloaded tree works
     * as well as a hand-renamed one.
     */
    private static String resolveGroundTruthSource(File sourceDir, DatasetPreset preset) {
        String[] candidates;
        if (preset == DatasetPreset.BIGANN) {
            candidates = new String[]{preset.groundTruthFile, "gnd/idx_1000M.ivecs"};
        } else if (preset == DatasetPreset.SIFT10M) {
            candidates = new String[]{preset.groundTruthFile, "gnd/idx_10M.ivecs"};
        } else {
            candidates = new String[]{preset.groundTruthFile};
        }
        for (String rel : candidates) {
            if (new File(sourceDir, rel).exists()
                    || new File(sourceDir, rel + ".gz").exists()) {
                return rel;
            }
        }
        // Nothing found — return preset default so stageFile produces the
        // normal error message.
        return preset.groundTruthFile;
    }

    private static boolean queryAndGroundTruthAlreadyPresent(File sourceDir, DatasetPreset preset) {
        if (preset != DatasetPreset.BIGANN && preset != DatasetPreset.SIFT10M) {
            return false; // let the loader decide
        }
        boolean queryHere = new File(sourceDir, preset.queryFile).exists()
                || new File(sourceDir, preset.queryFile + ".gz").exists();
        String gt = resolveGroundTruthSource(sourceDir, preset);
        boolean gtHere = new File(sourceDir, gt).exists()
                || new File(sourceDir, gt + ".gz").exists();
        return queryHere && gtHere;
    }

    /**
     * Stage {@code srcName} under {@code sourceDir} to {@code destName} under
     * {@code outputDir}. If the source file is only present as a {@code .gz},
     * decompress it; otherwise, try a hard link and fall back to a copy.
     */
    private static File stageFile(File sourceDir, String srcName, File outputDir, String destName)
            throws IOException {
        File dest = new File(outputDir, destName);
        if (dest.exists() && dest.length() > 0) {
            System.out.println("  [skip] already staged: " + dest.getName());
            return dest;
        }
        dest.getParentFile().mkdirs();

        File plain = new File(sourceDir, srcName);
        File gz = new File(sourceDir, srcName + ".gz");

        if (plain.exists()) {
            hardLinkOrCopy(plain, dest);
            System.out.println("  staged: " + dest.getName()
                    + " (" + formatSize(dest.length()) + ")");
            return dest;
        }
        if (gz.exists()) {
            System.out.println("  decompressing " + gz.getName() + " -> " + dest.getName() + " ...");
            gunzip(gz, dest);
            System.out.println("    " + dest.getName() + " ready (" + formatSize(dest.length()) + ")");
            return dest;
        }
        throw new IOException("Source file not found: " + plain.getAbsolutePath()
                + " (and no .gz variant)");
    }

    private static void hardLinkOrCopy(File src, File dest) throws IOException {
        try {
            Files.createLink(dest.toPath(), src.toPath());
            return;
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException | UnsupportedOperationException e) {
            // Cross-device link or filesystem without hard-link support; fall back to copy.
            System.out.println("  hard link failed (" + e.getMessage() + "), copying instead...");
            Files.copy(src.toPath(), dest.toPath());
        }
    }

    private static void gunzip(File gz, File dest) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(
                new BufferedInputStream(new FileInputStream(gz)));
             BufferedOutputStream out = new BufferedOutputStream(
                     new FileOutputStream(dest), 256 * 1024)) {
            byte[] buf = new byte[256 * 1024];
            long total = 0;
            int n;
            long nextReport = 256L * 1024 * 1024;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
                total += n;
                if (total >= nextReport) {
                    System.out.printf("    decompressed %s...%n", formatSize(total));
                    nextReport += 256L * 1024 * 1024;
                }
            }
        }
    }

    private static int readFirstInt(File f) throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(f), 16))) {
            byte[] b = new byte[4];
            dis.readFully(b);
            return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
    }

    private static long recordBytes(VecFormat format, int dimensions) {
        switch (format) {
            case FVECS:
                return 4L + 4L * dimensions;
            case BVECS:
                return 4L + dimensions;
            default:
                throw new IllegalArgumentException("Unsupported format for publisher: " + format);
        }
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format(Locale.ROOT, "%.1f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format(Locale.ROOT, "%.1f MB", mb);
        }
        return String.format(Locale.ROOT, "%.1f GB", mb / 1024.0);
    }

    /**
     * Invoke {@code push_dataset_gcs.sh} on the staged output directory.
     * Relies on {@code gsutil} being available on PATH.
     */
    private static void runPushScript(File outputDir, String gsPath) throws IOException, InterruptedException {
        File script = findPushScript();
        System.out.println();
        System.out.println("Uploading to " + gsPath + " using " + script.getAbsolutePath());
        ProcessBuilder pb = new ProcessBuilder(
                script.getAbsolutePath(),
                outputDir.getAbsolutePath(),
                gsPath)
                .inheritIO();
        int rc = pb.start().waitFor();
        if (rc != 0) {
            throw new IOException("push_dataset_gcs.sh exited with code " + rc);
        }
    }

    private static File findPushScript() throws IOException {
        // Look in a few conventional spots: CWD and the module root.
        File[] candidates = {
                new File("push_dataset_gcs.sh"),
                new File("vector-testings/push_dataset_gcs.sh"),
                new File("../push_dataset_gcs.sh")
        };
        for (File c : candidates) {
            if (c.isFile() && c.canExecute()) {
                return c.getAbsoluteFile();
            }
        }
        throw new IOException("Could not locate push_dataset_gcs.sh (searched: "
                + candidates[0].getAbsolutePath() + ", "
                + candidates[1].getAbsolutePath() + ", "
                + candidates[2].getAbsolutePath() + ")");
    }

    static final class PublisherConfig {
        DatasetPreset preset;
        String datasetName;
        String datasetDir = "./datasets";
        String outputDir;
        String gsPath;

        static PublisherConfig parse(String[] args) throws ParseException {
            Options opts = new Options();
            opts.addOption(null, "dataset", true, "Standard dataset to publish: bigann or gist1m (required)");
            opts.addOption(null, "dataset-dir", true, "Directory where the source dataset lives or will be downloaded (default: ./datasets)");
            opts.addOption(null, "output-dir", true, "Staging directory for the flat layout + descriptor (default: ./datasets/published/<dataset>)");
            opts.addOption(null, "gs-path", true, "Optional gs:// destination. When set, push_dataset_gcs.sh is invoked after staging.");
            opts.addOption("h", "help", false, "Show help");

            CommandLine cmd = new DefaultParser().parse(opts, args);
            if (cmd.hasOption("help") || !cmd.hasOption("dataset")) {
                new HelpFormatter().printHelp("standard-dataset-publisher", opts);
                if (!cmd.hasOption("help")) {
                    throw new ParseException("--dataset is required");
                }
                System.exit(0);
            }

            PublisherConfig cfg = new PublisherConfig();
            String name = cmd.getOptionValue("dataset").toLowerCase(Locale.ROOT);
            switch (name) {
                case "bigann":
                case "sift1b":
                    cfg.preset = DatasetPreset.BIGANN;
                    cfg.datasetName = "bigann";
                    break;
                case "gist1m":
                case "gist":
                    cfg.preset = DatasetPreset.GIST1M;
                    cfg.datasetName = "gist1m";
                    break;
                default:
                    throw new ParseException("Unsupported --dataset: " + name
                            + " (supported: bigann, gist1m)");
            }
            if (cmd.hasOption("dataset-dir")) {
                cfg.datasetDir = cmd.getOptionValue("dataset-dir");
            }
            cfg.outputDir = cmd.hasOption("output-dir")
                    ? cmd.getOptionValue("output-dir")
                    : cfg.datasetDir + "/published/" + cfg.datasetName;
            if (cmd.hasOption("gs-path")) {
                cfg.gsPath = cmd.getOptionValue("gs-path");
            }
            return cfg;
        }
    }

    /** Hidden — use {@link #main(String[])}. */
    private StandardDatasetPublisher() {
    }
}
