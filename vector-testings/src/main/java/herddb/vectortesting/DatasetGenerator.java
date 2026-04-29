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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Generates synthetic vector datasets by constructing random English sentences
 * and embedding them via Ollama. Outputs SIFT-compatible FVECS/IVECS files
 * with ground truth for recall evaluation.
 */
public class DatasetGenerator {

    public static void main(String[] args) throws Exception {
        GeneratorConfig config = GeneratorConfig.parse(args);
        System.out.println("Dataset Generator");
        System.out.println("Configuration: " + config);

        File outputDir = new File(config.outputDir);
        if (!outputDir.mkdirs() && !outputDir.isDirectory()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir.getAbsolutePath());
        }

        SentenceGenerator sentenceGen = new SentenceGenerator();

        // Derive file prefix from dataset name
        String prefix = config.name != null ? config.name : "generated";

        System.out.println("Connecting to Ollama at " + config.ollamaUrl + " with model '" + config.model + "'...");
        OllamaClient ollama = new OllamaClient(config.ollamaUrl, config.model);
        int dim = ollama.probeDimension();
        System.out.println("Embedding dimension: " + dim);

        File baseFile = new File(outputDir, prefix + "_base.fvecs");
        File queryFile = new File(outputDir, prefix + "_query.fvecs");
        File csvFile = new File(outputDir, prefix + "_sentences.csv");
        File descriptorFile = new File(outputDir, prefix + "_descriptor.json");

        // Resolve checkpoint plan: ascending base-vector counts → file. The final
        // checkpoint always equals config.total and uses the legacy
        // {prefix}_groundtruth.ivecs name, so old descriptor consumers keep working.
        long[] checkpointCounts = config.groundTruthCheckpoints;
        Map<Long, File> checkpointFiles = new LinkedHashMap<>();
        for (long count : checkpointCounts) {
            String fileName = (count == config.total)
                    ? prefix + "_groundtruth.ivecs"
                    : prefix + "_groundtruth_" + count + ".ivecs";
            checkpointFiles.put(count, new File(outputDir, fileName));
        }

        // Buffer first numQueries vectors in memory for ground truth tracker
        List<float[]> queryVectorsList = new ArrayList<>(config.numQueries);
        GroundTruthTracker tracker = null;

        long startTime = System.currentTimeMillis();
        // Index of the next checkpoint to emit; advances each time we cross one.
        int nextCheckpointIdx = 0;

        try (SiftWriter baseWriter = new SiftWriter(baseFile);
             SiftWriter queryWriter = new SiftWriter(queryFile);
             PrintWriter csvWriter = config.csv
                     ? new PrintWriter(csvFile, StandardCharsets.UTF_8)
                     : null) {

            if (csvWriter != null) {
                csvWriter.println("id,sentence,vector");
            }

            int generated = 0;
            while (generated < config.total) {
                int batchCount = Math.min(config.batchSize, config.total - generated);
                List<String> sentences = sentenceGen.generateBatch(batchCount);
                float[][] embeddings = ollama.embed(sentences);

                for (int i = 0; i < embeddings.length; i++) {
                    int globalIdx = generated + i;
                    float[] vec = embeddings[i];

                    baseWriter.writeFvec(vec);

                    if (globalIdx < config.numQueries) {
                        queryWriter.writeFvec(vec);
                        queryVectorsList.add(vec);
                    }

                    // Initialize tracker once we have all query vectors
                    if (globalIdx == config.numQueries - 1) {
                        float[][] queryVectors = queryVectorsList.toArray(new float[0][]);
                        tracker = new GroundTruthTracker(queryVectors, config.groundTruthK, config.similarity);
                        // Offer all query vectors to the tracker (they are part of the base set)
                        for (int j = 0; j < queryVectors.length; j++) {
                            tracker.offer(j, queryVectors[j]);
                        }
                    } else if (tracker != null) {
                        tracker.offer(globalIdx, vec);
                    }

                    if (csvWriter != null) {
                        csvWriter.print(globalIdx);
                        csvWriter.print(',');
                        csvWriter.print(escapeCsv(sentences.get(i)));
                        csvWriter.print(',');
                        csvWriter.println(vectorToString(vec));
                    }

                    // Emit any pending checkpoints we just crossed. Multiple checkpoints can
                    // theoretically share a count if the user's CSV is malformed, but
                    // GeneratorConfig.parseCheckpoints already rejects duplicates, so this is
                    // a single iteration in practice.
                    long crossedCount = (long) globalIdx + 1L;
                    while (tracker != null
                            && nextCheckpointIdx < checkpointCounts.length
                            && checkpointCounts[nextCheckpointIdx] == crossedCount) {
                        long count = checkpointCounts[nextCheckpointIdx];
                        File gtFile = checkpointFiles.get(count);
                        writeGroundTruthFile(gtFile, tracker);
                        System.out.printf("%n  Wrote ground truth checkpoint @ %,d vectors → %s%n",
                                count, gtFile.getName());
                        nextCheckpointIdx++;
                    }
                }

                generated += embeddings.length;
                long elapsed = System.currentTimeMillis() - startTime;
                double rate = generated * 1000.0 / elapsed;
                double eta = (config.total - generated) / rate;
                System.out.printf("\r  Generated %,d / %,d vectors (%.0f vec/s, ETA: %.0fs)    ",
                        generated, config.total, rate, eta);
            }
            System.out.println();
        }

        // Defensive fallback: if no checkpoint was emitted (e.g. a future code path leaves
        // total < numQueries), still write the final ground-truth file at config.total so
        // older consumers find {prefix}_groundtruth.ivecs.
        if (nextCheckpointIdx < checkpointCounts.length) {
            if (tracker == null && !queryVectorsList.isEmpty()) {
                float[][] queryVectors = queryVectorsList.toArray(new float[0][]);
                tracker = new GroundTruthTracker(queryVectors, config.groundTruthK, config.similarity);
                for (int j = 0; j < queryVectors.length; j++) {
                    tracker.offer(j, queryVectors[j]);
                }
            }
            while (nextCheckpointIdx < checkpointCounts.length && tracker != null) {
                long count = checkpointCounts[nextCheckpointIdx];
                File gtFile = checkpointFiles.get(count);
                writeGroundTruthFile(gtFile, tracker);
                System.out.printf("  Wrote ground truth checkpoint @ %,d vectors → %s%n",
                        count, gtFile.getName());
                nextCheckpointIdx++;
            }
        }

        // Write dataset descriptor
        System.out.println("Writing dataset descriptor...");
        writeDescriptor(descriptorFile, config, prefix, dim, checkpointFiles);

        // Optional ZIP compression
        File zipFile = null;
        if (config.zip) {
            zipFile = new File(outputDir, prefix + "_dataset.zip");
            System.out.println("Creating ZIP archive: " + zipFile.getName());
            List<File> zipEntries = new ArrayList<>();
            zipEntries.add(baseFile);
            zipEntries.add(queryFile);
            for (File gt : checkpointFiles.values()) {
                zipEntries.add(gt);
            }
            zipEntries.add(descriptorFile);
            if (config.csv) {
                zipEntries.add(csvFile);
            }
            createZip(zipFile, zipEntries.toArray(new File[0]));

            // Remove individual files — the ZIP is the single deliverable
            System.out.println("Removing individual files (kept in ZIP)...");
            deleteQuietly(baseFile);
            deleteQuietly(queryFile);
            for (File gt : checkpointFiles.values()) {
                deleteQuietly(gt);
            }
            deleteQuietly(descriptorFile);
            deleteQuietly(csvFile);
        }

        long totalTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println();
        System.out.println("Generation complete in " + totalTime + "s");
        System.out.println("Output directory: " + outputDir.getAbsolutePath());
        if (config.zip) {
            System.out.println("  ZIP archive:    " + zipFile.getName() + " (" + formatSize(zipFile.length()) + ")");
        } else {
            System.out.println("  Descriptor:     " + descriptorFile.getName());
            System.out.println("  Base vectors:   " + baseFile.getName() + " (" + formatSize(baseFile.length()) + ")");
            System.out.println("  Query vectors:  " + queryFile.getName() + " (" + formatSize(queryFile.length()) + ")");
            for (Map.Entry<Long, File> e : checkpointFiles.entrySet()) {
                File gt = e.getValue();
                System.out.printf("  Ground truth @ %,d: %s (%s)%n",
                        e.getKey(), gt.getName(), formatSize(gt.length()));
            }
            if (config.csv) {
                System.out.println("  CSV:            " + csvFile.getName() + " (" + formatSize(csvFile.length()) + ")");
            }
        }
    }

    /**
     * Writes one IVECS ground-truth file using the tracker's current snapshot. Safe to
     * call multiple times during a generation run — {@link GroundTruthTracker#getGroundTruth()}
     * is non-destructive.
     */
    private static void writeGroundTruthFile(File gtFile, GroundTruthTracker tracker) throws Exception {
        int[][] groundTruth = tracker.getGroundTruth();
        try (SiftWriter gtWriter = new SiftWriter(gtFile)) {
            for (int[] row : groundTruth) {
                gtWriter.writeIvec(row);
            }
        }
    }

    private static void writeDescriptor(File descriptorFile, GeneratorConfig config,
                                          String prefix, int dimensions,
                                          Map<Long, File> checkpointFiles) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        ObjectNode root = mapper.createObjectNode();
        root.put("name", config.name != null ? config.name : prefix);
        root.put("format", "fvecs");
        root.put("dimensions", dimensions);
        root.put("similarity", config.similarity);
        root.put("totalVectors", config.total);
        root.put("numQueries", config.numQueries);
        root.put("groundTruthK", config.groundTruthK);
        root.put("embeddingModel", config.model);
        root.put("baseFile", prefix + "_base.fvecs");
        root.put("queryFile", prefix + "_query.fvecs");
        // Legacy field: always points to the ground-truth file matching --total. This
        // duplicates the last entry of groundTruthCheckpoints, but old descriptor
        // consumers only look at this field.
        root.put("groundTruthFile", prefix + "_groundtruth.ivecs");
        // New field: list of (baseVectorCount, file) pairs in ascending order, including
        // the final entry whose file equals groundTruthFile. Consumers that want recall
        // for prefix runs (--rows N) look up the matching baseVectorCount here.
        ArrayNode checkpoints = root.putArray("groundTruthCheckpoints");
        for (Map.Entry<Long, File> e : checkpointFiles.entrySet()) {
            ObjectNode entry = checkpoints.addObject();
            entry.put("baseVectorCount", e.getKey());
            entry.put("file", e.getValue().getName());
        }
        root.put("createdAt", Instant.now().toString());
        mapper.writeValue(descriptorFile, root);
    }

    private static void createZip(File zipFile, File... files) throws Exception {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile))) {
            byte[] buffer = new byte[256 * 1024];
            for (File file : files) {
                if (file == null || !file.exists()) {
                    continue;
                }
                zos.putNextEntry(new ZipEntry(file.getName()));
                try (FileInputStream fis = new FileInputStream(file)) {
                    int len;
                    while ((len = fis.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                }
                zos.closeEntry();
            }
        }
    }

    private static void deleteQuietly(File file) {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    private static String escapeCsv(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static String vectorToString(float[] vec) {
        StringBuilder sb = new StringBuilder();
        sb.append('"').append('[');
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(vec[i]);
        }
        sb.append(']').append('"');
        return sb.toString();
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }
}
