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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class DatasetLoader {

    private final String datasetDir;
    private final DatasetPreset preset;
    private final String datasetUrl;

    public DatasetLoader(String datasetDir, DatasetPreset preset, String datasetUrl) {
        this.datasetDir = datasetDir;
        this.preset = preset;
        this.datasetUrl = datasetUrl != null ? datasetUrl : preset.defaultUrl;
    }

    public File getDatasetSubDir() {
        return new File(datasetDir, preset.subDir);
    }

    public void ensureDataset() throws IOException {
        if (preset == DatasetPreset.CUSTOM) {
            ensureCustomDataset();
            return;
        }
        File dir = getDatasetSubDir();
        File baseFile = new File(dir, preset.baseFile);
        File baseFileGz = new File(dir, preset.baseFile + ".gz");
        if (baseFile.exists() || baseFileGz.exists()) {
            System.out.println("Dataset already present at " + dir.getAbsolutePath());
            return;
        }

        dir.mkdirs();
        String archiveName = datasetUrl.substring(datasetUrl.lastIndexOf('/') + 1);
        File archive = new File(datasetDir, archiveName);

        if (!archive.exists()) {
            System.out.println("Downloading dataset from " + datasetUrl + " ...");
            downloadFile(datasetUrl, archive);
            System.out.println("Download complete: " + archive.length() + " bytes");
        }

        if (archiveName.endsWith(".tar.gz") || archiveName.endsWith(".tgz")) {
            System.out.println("Extracting " + archive.getAbsolutePath() + " ...");
            extractTarGz(archive, new File(datasetDir));
            System.out.println("Extraction complete.");
            if (!baseFile.exists()) {
                throw new IOException("Expected file not found after extraction: " + baseFile.getAbsolutePath());
            }
        } else if (archiveName.endsWith(".gz")) {
            // Keep the .gz file to save disk space — load methods decompress on-the-fly
            if (!archive.renameTo(baseFileGz)) {
                throw new IOException("Failed to move " + archive + " to " + baseFileGz);
            }
        } else {
            // Plain file (e.g. HDF5) — move into the dataset subdirectory
            File target = new File(dir, archiveName);
            if (!archive.renameTo(target)) {
                // renameTo can fail across filesystems, fall back to copy
                Files.copy(archive.toPath(), target.toPath());
                archive.delete();
            }
        }
    }

    public void ensureQueryAndGroundTruth() throws IOException {
        // For CUSTOM and HDF5 datasets, query and ground truth are already present
        if (preset == DatasetPreset.CUSTOM || preset.baseFormat == VecFormat.HDF5) {
            return;
        }

        File dir = getDatasetSubDir();

        // For BIGANN/SIFT10M, query and ground truth are separate downloads
        if (preset == DatasetPreset.BIGANN || preset == DatasetPreset.SIFT10M) {
            ensureBigannFile("bigann_query.bvecs",
                    "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs.gz");
            ensureBigannFile(preset.groundTruthFile,
                    "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_gnd.tar.gz");
        }
    }

    private void ensureBigannFile(String targetFile, String url) throws IOException {
        File dir = getDatasetSubDir();
        File target = new File(dir, targetFile);
        File targetGz = new File(dir, targetFile + ".gz");
        if (target.exists() || targetGz.exists()) {
            return;
        }

        String archiveName = url.substring(url.lastIndexOf('/') + 1);
        File archive = new File(datasetDir, archiveName);

        if (!archive.exists()) {
            System.out.println("Downloading " + archiveName + " from " + url + " ...");
            downloadFile(url, archive);
        }

        if (archiveName.endsWith(".tar.gz")) {
            extractTarGz(archive, dir);
        } else if (archiveName.endsWith(".gz")) {
            // Keep the .gz file to save disk space — load methods decompress on-the-fly
            targetGz.getParentFile().mkdirs();
            if (!archive.renameTo(targetGz)) {
                throw new IOException("Failed to move " + archive + " to " + targetGz);
            }
        }
    }

    /**
     * For CUSTOM datasets, the datasetUrl points to either:
     * <ul>
     *   <li>A ZIP file (e.g., gs://bucket/path/my_dataset.zip) containing the
     *       descriptor JSON and all data files</li>
     *   <li>A descriptor JSON file (e.g., gs://bucket/path/my_descriptor.json)
     *       with sibling data files at the same URL path</li>
     * </ul>
     */
    private void ensureCustomDataset() throws IOException {
        File dir = getDatasetSubDir();
        dir.mkdirs();

        // Check if descriptor already exists locally
        File[] existing = dir.listFiles((d, name) -> name.endsWith("_descriptor.json"));
        if (existing != null && existing.length > 0) {
            System.out.println("Custom dataset already present at " + dir.getAbsolutePath());
            return;
        }

        if (datasetUrl == null || datasetUrl.isEmpty()) {
            throw new IOException("CUSTOM dataset requires --dataset-url pointing to a ZIP file or "
                    + "descriptor JSON URL (e.g., gs://bucket/path/dataset.zip or "
                    + "gs://bucket/path/my_descriptor.json)");
        }

        if (datasetUrl.endsWith(".zip")) {
            ensureCustomDatasetFromZip(dir);
        } else {
            ensureCustomDatasetFromDescriptor(dir);
        }
    }

    private void ensureCustomDatasetFromZip(File dir) throws IOException {
        String zipUrl = datasetUrl;
        String zipName = zipUrl.substring(zipUrl.lastIndexOf('/') + 1);
        File zipFile = new File(dir, zipName);

        if (!zipFile.exists()) {
            System.out.println("Downloading dataset ZIP from " + zipUrl + " ...");
            downloadSmartUrl(zipUrl, zipFile);
            System.out.println("Download complete: " + zipFile.length() + " bytes");
        }

        System.out.println("Extracting " + zipName + " ...");
        extractZip(zipFile, dir);
        System.out.println("Extraction complete.");

        // Verify descriptor was extracted
        File[] descriptors = dir.listFiles((d, name) -> name.endsWith("_descriptor.json"));
        if (descriptors == null || descriptors.length == 0) {
            throw new IOException("ZIP file does not contain a descriptor JSON: " + zipName);
        }
    }

    private void ensureCustomDatasetFromDescriptor(File dir) throws IOException {
        String descriptorUrl = datasetUrl;
        String baseUrl = descriptorUrl.substring(0, descriptorUrl.lastIndexOf('/'));

        // Download descriptor
        String descriptorName = descriptorUrl.substring(descriptorUrl.lastIndexOf('/') + 1);
        File descriptorFile = new File(dir, descriptorName);
        System.out.println("Downloading descriptor from " + descriptorUrl + " ...");
        downloadSmartUrl(descriptorUrl, descriptorFile);

        // Parse descriptor to find data files
        DatasetDescriptor desc = DatasetDescriptor.load(descriptorFile);

        // Build a deduped list of files to fetch: base + query + legacy ground-truth
        // file + every entry from groundTruthCheckpoints. The last checkpoint's file
        // typically equals desc.groundTruthFile, so a LinkedHashSet keeps insertion
        // order while suppressing the duplicate.
        java.util.LinkedHashSet<String> filesToFetch = new java.util.LinkedHashSet<>();
        if (desc.baseFile != null) {
            filesToFetch.add(desc.baseFile);
        }
        if (desc.queryFile != null) {
            filesToFetch.add(desc.queryFile);
        }
        if (desc.groundTruthFile != null) {
            filesToFetch.add(desc.groundTruthFile);
        }
        for (DatasetDescriptor.GroundTruthCheckpoint cp : desc.groundTruthCheckpoints) {
            filesToFetch.add(cp.file);
        }

        for (String fileName : filesToFetch) {
            File target = new File(dir, fileName);
            if (!target.exists()) {
                String fileUrl = baseUrl + "/" + fileName;
                System.out.println("Downloading " + fileName + " ...");
                downloadSmartUrl(fileUrl, target);
            }
        }
        System.out.println("Custom dataset download complete.");
    }

    private static void extractZip(File zipFile, File destDir) throws IOException {
        byte[] buffer = new byte[256 * 1024];
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFile)))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                // Flatten: extract all files directly into destDir regardless of ZIP path
                String name = entry.getName();
                if (name.contains("/")) {
                    name = name.substring(name.lastIndexOf('/') + 1);
                }
                File out = new File(destDir, name);
                System.out.println("  Extracting: " + name);
                try (FileOutputStream fos = new FileOutputStream(out);
                     BufferedOutputStream bos = new BufferedOutputStream(fos, 256 * 1024)) {
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        bos.write(buffer, 0, len);
                    }
                }
                zis.closeEntry();
            }
        }
    }

    /**
     * Downloads a file, routing gs:// URLs through the S3-compatible GCS API
     * and all other URLs through HTTP/FTP.
     *
     * <p>Package-private so the {@code DatasetDescribe} CLI tool can reuse the
     * same URL-resolution path as the bench loader.</p>
     */
    void downloadSmartUrl(String url, File dest) throws IOException {
        if (url.startsWith("gs://")) {
            downloadGsFile(url, dest);
        } else {
            downloadFile(url, dest);
        }
    }

    /**
     * Downloads a file from Google Cloud Storage using the S3-compatible API.
     * Uses HMAC credentials from GCS_ACCESS_KEY / GCS_SECRET_KEY environment
     * variables when available, or anonymous access for public buckets.
     */
    private static void downloadGsFile(String gsUrl, File dest) throws IOException {
        dest.getParentFile().mkdirs();

        // Parse gs://bucket/key
        String path = gsUrl.substring("gs://".length());
        int slash = path.indexOf('/');
        if (slash <= 0) {
            throw new IOException("Invalid gs:// URL (expected gs://bucket/key): " + gsUrl);
        }
        String bucket = path.substring(0, slash);
        String key = path.substring(slash + 1);

        String accessKey = System.getenv("GCS_ACCESS_KEY");
        String secretKey = System.getenv("GCS_SECRET_KEY");
        boolean hasCredentials = accessKey != null && !accessKey.isEmpty()
                && secretKey != null && !secretKey.isEmpty();

        System.out.println("  Downloading from " + gsUrl + " via S3-compatible API"
                + (hasCredentials ? " (HMAC credentials)" : " (anonymous)"));

        var clientBuilder = S3Client.builder()
                .region(Region.of("auto"))
                .endpointOverride(URI.create("https://storage.googleapis.com"))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build());

        if (hasCredentials) {
            clientBuilder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        } else {
            clientBuilder.credentialsProvider(AnonymousCredentialsProvider.create());
        }

        try (S3Client s3 = clientBuilder.build()) {
            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            try (InputStream in = s3.getObject(req);
                 FileOutputStream out = new FileOutputStream(dest)) {
                byte[] buf = new byte[8192];
                long totalBytes = 0;
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    totalBytes += n;
                    if (totalBytes % (50 * 1024 * 1024) < 8192) {
                        System.out.printf("    Downloaded %.1f MB...%n", totalBytes / (1024.0 * 1024.0));
                    }
                }
                System.out.printf("    Download complete: %.1f MB%n", totalBytes / (1024.0 * 1024.0));
            }
        }

        if (!dest.exists()) {
            throw new IOException("S3 download reported success but file not found: " + dest);
        }
    }

    private static final int MAX_DOWNLOAD_RETRIES = 10;

    private void downloadFile(String urlStr, File dest) throws IOException {
        dest.getParentFile().mkdirs();
        File part = new File(dest.getParent(), dest.getName() + ".part");
        boolean success = false;
        IOException lastException = null;
        for (int attempt = 0; attempt <= MAX_DOWNLOAD_RETRIES; attempt++) {
            if (attempt > 0) {
                System.out.printf("  Retry %d/%d after error: %s%n",
                        attempt, MAX_DOWNLOAD_RETRIES, lastException.getMessage());
            }
            try {
                downloadFileAttempt(urlStr, part);
                success = true;
                break;
            } catch (IOException e) {
                lastException = e;
            }
        }
        if (!success) {
            throw new IOException("Download failed after " + MAX_DOWNLOAD_RETRIES
                    + " retries", lastException);
        }
        if (!part.renameTo(dest)) {
            Files.copy(part.toPath(), dest.toPath());
            part.delete();
        }
    }

    private void downloadFileAttempt(String urlStr, File part) throws IOException {
        long resumeFrom = part.exists() ? part.length() : 0;
        if (resumeFrom > 0) {
            System.out.printf("  Resuming download from %.1f MB%n",
                    resumeFrom / (1024.0 * 1024.0));
        }

        if (urlStr.startsWith("ftp://")) {
            downloadFileAttemptFtp(urlStr, part, resumeFrom);
        } else {
            downloadFileAttemptHttp(urlStr, part, resumeFrom);
        }
    }

    private void downloadFileAttemptHttp(String urlStr, File part, long resumeFrom) throws IOException {
        URL url = new URL(urlStr);
        URLConnection conn = url.openConnection();
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(60_000);

        long totalFileSize = -1;
        boolean appending = false;

        if (conn instanceof HttpURLConnection) {
            HttpURLConnection http = (HttpURLConnection) conn;
            if (resumeFrom > 0) {
                http.setRequestProperty("Range", "bytes=" + resumeFrom + "-");
            }
            http.connect();
            int status = http.getResponseCode();
            if (status == 206) {
                long remaining = http.getContentLengthLong();
                totalFileSize = remaining >= 0 ? resumeFrom + remaining : -1;
                appending = true;
            } else if (status == 200) {
                if (resumeFrom > 0) {
                    System.out.println("  Server doesn't support resume, restarting download...");
                    part.delete();
                    resumeFrom = 0;
                }
                totalFileSize = http.getContentLengthLong();
                appending = false;
            } else {
                throw new IOException("HTTP " + status + " downloading " + urlStr);
            }
        } else {
            // Unknown protocol — no resume support
            if (resumeFrom > 0) {
                System.out.println("  Cannot resume this download, restarting...");
                part.delete();
                resumeFrom = 0;
            }
            conn.connect();
            totalFileSize = conn.getContentLengthLong();
            appending = false;
        }

        try (InputStream in = new BufferedInputStream(conn.getInputStream(), 256 * 1024);
             FileOutputStream fos = new FileOutputStream(part, appending);
             BufferedOutputStream out = new BufferedOutputStream(fos, 256 * 1024)) {
            transferStream(in, out, resumeFrom, totalFileSize);
        }
    }

    private void downloadFileAttemptFtp(String urlStr, File part, long resumeFrom) throws IOException {
        URI uri = URI.create(urlStr);
        FTPClient ftp = new FTPClient();
        ftp.setConnectTimeout(30_000);
        ftp.setDefaultTimeout(60_000);
        ftp.setDataTimeout(Duration.ofMillis(60_000));
        ftp.connect(uri.getHost(), uri.getPort() < 0 ? 21 : uri.getPort());
        ftp.login("anonymous", "anonymous@example.com");
        ftp.enterLocalPassiveMode();
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
        if (resumeFrom > 0) {
            ftp.setRestartOffset(resumeFrom);
        }
        String sizeReply = ftp.getSize(uri.getPath());
        long totalFileSize = -1;
        if (sizeReply != null) {
            try {
                totalFileSize = Long.parseLong(sizeReply.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        InputStream ftpStream = ftp.retrieveFileStream(uri.getPath());
        if (ftpStream == null) {
            ftp.disconnect();
            throw new IOException("FTP retrieve failed: " + ftp.getReplyString());
        }
        try (InputStream in = new BufferedInputStream(ftpStream, 256 * 1024);
             FileOutputStream fos = new FileOutputStream(part, resumeFrom > 0);
             BufferedOutputStream out = new BufferedOutputStream(fos, 256 * 1024)) {
            transferStream(in, out, resumeFrom, totalFileSize);
        } finally {
            ftp.completePendingCommand();
            ftp.logout();
            ftp.disconnect();
        }
    }

    private void transferStream(InputStream in, BufferedOutputStream out,
                                long resumeFrom, long totalFileSize) throws IOException {
        byte[] buf = new byte[65536];
        int read;
        long total = resumeFrom;
        long intervalStart = System.currentTimeMillis();
        long intervalBytes = 0;
        long nextPrint = ((resumeFrom / (50L * 1024 * 1024)) + 1) * 50L * 1024 * 1024;

        while ((read = in.read(buf)) != -1) {
            out.write(buf, 0, read);
            total += read;
            intervalBytes += read;

            if (total >= nextPrint) {
                long now = System.currentTimeMillis();
                double elapsedSec = Math.max((now - intervalStart) / 1000.0, 0.001);
                double mbps = (intervalBytes / (1024.0 * 1024.0)) / elapsedSec;
                if (totalFileSize > 0) {
                    double pct = 100.0 * total / totalFileSize;
                    System.out.printf("  Downloaded %.1f / %.1f MB (%.1f%%) — %.2f MB/s%n",
                            total / (1024.0 * 1024.0), totalFileSize / (1024.0 * 1024.0),
                            pct, mbps);
                } else {
                    System.out.printf("  Downloaded %.1f MB — %.2f MB/s%n",
                            total / (1024.0 * 1024.0), mbps);
                }
                intervalStart = now;
                intervalBytes = 0;
                nextPrint += 50L * 1024 * 1024;
            }
        }
    }

    private void extractTarGz(File tarGz, File destDir) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("tar", "xzf", tarGz.getAbsolutePath())
                .directory(destDir)
                .inheritIO();
        try {
            int exitCode = pb.start().waitFor();
            if (exitCode != 0) {
                throw new IOException("tar extraction failed with exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("tar extraction interrupted", e);
        }
    }

    private InputStream openInputStream(File file) throws IOException {
        if (!file.exists()) {
            File gz = new File(file.getParent(), file.getName() + ".gz");
            if (gz.exists()) {
                return new GZIPInputStream(new BufferedInputStream(new FileInputStream(gz)));
            }
        }
        return new BufferedInputStream(new FileInputStream(file));
    }

    // ---- HDF5 loading ----

    private File getHdf5File() {
        return new File(getDatasetSubDir(), preset.baseFile);
    }

    private List<float[]> loadHdf5Vectors(String datasetPath, int maxVectors) {
        try (HdfFile hdf = new HdfFile(getHdf5File().toPath())) {
            Dataset ds = hdf.getDatasetByPath(datasetPath);
            float[][] data = (float[][]) ds.getData();
            int count = Math.min(maxVectors, data.length);
            List<float[]> vectors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                vectors.add(data[i]);
            }
            return vectors;
        }
    }

    private List<int[]> loadHdf5GroundTruth(int maxVectors) {
        try (HdfFile hdf = new HdfFile(getHdf5File().toPath())) {
            Dataset ds = hdf.getDatasetByPath("neighbors");
            int[][] data = (int[][]) ds.getData();
            int count = Math.min(maxVectors, data.length);
            List<int[]> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                result.add(data[i]);
            }
            return result;
        }
    }

    // ---- public loading API ----

    public List<float[]> loadBaseVectors(int maxVectors) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return loadHdf5Vectors("train", maxVectors);
        }
        String baseFileName = preset == DatasetPreset.CUSTOM && customDescriptor != null
                ? customDescriptor.baseFile : preset.baseFile;
        File file = new File(getDatasetSubDir(), baseFileName);
        VecFormat format = resolveBaseFormat();
        if (format == VecFormat.BVECS) {
            return loadBvecs(openInputStream(file), maxVectors);
        } else {
            return loadFvecs(openInputStream(file), maxVectors);
        }
    }

    /**
     * Resolve the on-disk format for the base/query vector files. For CUSTOM
     * datasets the format comes from the descriptor JSON; for other presets it
     * is hard-coded in the enum.
     */
    private VecFormat resolveBaseFormat() {
        if (preset == DatasetPreset.CUSTOM && customDescriptor != null) {
            return customDescriptor.format;
        }
        return preset.baseFormat;
    }

    private VecFormat resolveQueryFormat() {
        if (preset == DatasetPreset.CUSTOM && customDescriptor != null) {
            return customDescriptor.format;
        }
        return preset.queryFormat;
    }

    public interface VectorStream extends Iterable<float[]>, Closeable {}

    public VectorStream streamBaseVectors(long maxVectors) throws IOException {
        return streamBaseVectors(0L, maxVectors);
    }

    /**
     * Stream up to {@code maxVectors} base vectors, skipping the first {@code skipVectors}.
     * Row IDs should start from {@code skipVectors} when resuming ingestion.
     */
    public VectorStream streamBaseVectors(long skipVectors, long maxVectors) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return streamHdf5Vectors(skipVectors, maxVectors);
        }
        String baseFileName = preset == DatasetPreset.CUSTOM && customDescriptor != null
                ? customDescriptor.baseFile : preset.baseFile;
        File file = new File(getDatasetSubDir(), baseFileName);
        InputStream in = openInputStream(file);
        DataInputStream dis = new DataInputStream(in);
        VecFormat format = resolveBaseFormat();

        if (skipVectors > 0) {
            System.out.println("  Skipping " + skipVectors + " vectors to resume from position " + skipVectors + "...");
            try {
                skipVecsInStream(dis, format, skipVectors);
            } catch (IOException e) {
                // The new "loud EOF" semantics in skipExactly makes this
                // throw path reachable on corrupt / truncated files; close
                // the file handle before propagating so a partially-built
                // VectorStream doesn't leak the FD across retries.
                try {
                    dis.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
                throw e;
            }
            System.out.println("  Skip complete.");
        }

        return new VectorStream() {
            @Override
            public void close() throws IOException {
                dis.close();
            }

            @Override
            public Iterator<float[]> iterator() {
                return new Iterator<float[]>() {
                    private long count = 0;
                    private float[] next = null;
                    private boolean eof = false;
                    /**
                     * Reusable byte-buffer for the raw payload; allocated
                     * lazily on the first read once dim is known. Issue
                     * #443: hoisting the byte[] out of the per-vector
                     * read loop eliminates ~4 KB of garbage per vector at
                     * dim=1024 (~400 GB on a 100 M ingest).
                     */
                    private byte[] scratchBytes;
                    /**
                     * Reusable little-endian FloatBuffer view over
                     * {@link #scratchBytes}; only populated for FVECS.
                     * Saves both the {@code ByteBuffer.wrap(...)} and
                     * {@code asFloatBuffer()} wrapper allocations per
                     * vector.
                     */
                    private FloatBuffer scratchFloatView;
                    /**
                     * Cached dim from the previous vector; SIFT files
                     * have a constant dim, so reusing the scratch is safe
                     * unless we encounter a file that violates the
                     * invariant — in which case we transparently
                     * reallocate.
                     */
                    private int cachedDim = -1;

                    private float[] readNext() {
                        if (count >= maxVectors || eof) {
                            return null;
                        }
                        try {
                            int dim;
                            try {
                                dim = readLittleEndianInt(dis);
                            } catch (java.io.EOFException e) {
                                eof = true;
                                return null;
                            }
                            ensureScratch(dim);
                            dis.readFully(scratchBytes);
                            // float[] is the one allocation we cannot
                            // avoid — the iterator contract hands the
                            // array to the caller, who keeps it.
                            float[] vec = new float[dim];
                            if (format == VecFormat.BVECS) {
                                for (int i = 0; i < dim; i++) {
                                    vec[i] = (scratchBytes[i] & 0xFF);
                                }
                            } else {
                                // Issue #443: bulk decode via reused
                                // FloatBuffer view. The JIT can vectorize
                                // the underlying byte→float conversion
                                // and the wrappers are not re-allocated.
                                scratchFloatView.rewind();
                                scratchFloatView.get(vec);
                            }
                            return vec;
                        } catch (IOException e) {
                            eof = true;
                            throw new RuntimeException("Error reading vector stream", e);
                        }
                    }

                    private void ensureScratch(int dim) {
                        if (scratchBytes != null && cachedDim == dim) {
                            return;
                        }
                        cachedDim = dim;
                        int payloadBytes = (format == VecFormat.BVECS) ? dim : dim * 4;
                        scratchBytes = new byte[payloadBytes];
                        if (format != VecFormat.BVECS) {
                            scratchFloatView = ByteBuffer.wrap(scratchBytes)
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .asFloatBuffer();
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        if (next == null && !eof && count < maxVectors) {
                            next = readNext();
                        }
                        return next != null;
                    }

                    @Override
                    public float[] next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        float[] v = next;
                        next = null;
                        count++;
                        if (count % 1_000_000 == 0) {
                            System.out.println("  Streamed " + count + " vectors...");
                        }
                        return v;
                    }
                };
            }
        };
    }

    /**
     * Skip {@code count} vectors in-place from a DataInputStream of FVECS
     * or BVECS format.
     *
     * <p><b>Correctness requirement:</b> the SIFT format guarantees a
     * constant {@code dim} across every record in a single file. The
     * bulk-skip implementation below relies on this — it reads dim from
     * the first record only, computes the per-record byte size, and then
     * skips the remaining {@code count - 1} records as a single
     * {@link DataInputStream#skip(long)}. A file that violates the
     * constant-dim invariant would silently land at the wrong byte
     * offset, so do <em>not</em> introduce a partial-fallback path here
     * that re-reads dim per record without also detecting and surfacing
     * the inconsistency loudly.
     *
     * <p>Issue #443: the previous per-vector {@code readInt + skip} loop
     * was wasted work — for a 100 M-row resume that was 100 M small
     * reads followed by 100 M skip calls. Folding into one bulk skip
     * lets the underlying {@code BufferedInputStream} (or
     * {@code GZIPInputStream}) short-circuit via {@code
     * FileInputStream.skip()} where supported.
     */
    private static void skipVecsInStream(DataInputStream dis, VecFormat format, long count) throws IOException {
        if (count <= 0) {
            return;
        }
        // Read dim from the first record. SIFT format guarantees this
        // value is identical for every subsequent record in the same file.
        int dim = readLittleEndianInt(dis);
        long payloadBytes = (format == VecFormat.BVECS) ? dim : (long) dim * 4;
        long recordBytes = 4L + payloadBytes;

        // Skip the rest of the first record's payload.
        skipExactly(dis, payloadBytes, "first vector payload");

        // Bulk-skip the remaining (count - 1) records.
        long remainingRecords = count - 1L;
        if (remainingRecords > 0) {
            long bytesToSkip = remainingRecords * recordBytes;
            skipExactly(dis, bytesToSkip, "bulk-skip of " + remainingRecords + " vectors");
        }
        System.out.println("  Skipped " + count + " vectors (dim=" + dim + ").");
    }

    /**
     * Skip exactly {@code totalBytes} bytes from {@code dis}, throwing
     * if EOF is reached early.
     *
     * <p>Implemented via chunked {@link DataInputStream#readFully(byte[],
     * int, int)} into a 64 KB scratch buffer rather than
     * {@link InputStream#skip(long)}. Reason: {@code FileInputStream.skip}
     * delegates to {@code lseek}, which on most platforms succeeds even
     * when seeking past EOF (the next {@code read()} returns -1, but the
     * {@code skip()} itself does not throw). A bulk-skip that silently
     * lands past EOF would let a corrupted / short file slip through the
     * resume path undetected. {@code readFully} reads bytes through user
     * space and surfaces EOF as a loud {@link java.io.EOFException},
     * which we re-wrap into an actionable {@link IOException} below.
     *
     * <p>The throughput cost vs raw {@code skip()} is meaningful (a few
     * hundred MB/s of memcpy), but resume is a one-time startup cost so
     * we trade it for reliable error reporting.
     */
    private static void skipExactly(DataInputStream dis, long totalBytes, String what) throws IOException {
        if (totalBytes <= 0) {
            return;
        }
        int bufSize = (int) Math.min(64L * 1024L, totalBytes);
        byte[] discard = new byte[bufSize];
        long remaining = totalBytes;
        while (remaining > 0) {
            int chunk = (int) Math.min((long) bufSize, remaining);
            try {
                dis.readFully(discard, 0, chunk);
            } catch (java.io.EOFException e) {
                throw new IOException("Unexpected end of stream while skipping " + what
                        + " (asked for " + totalBytes + " bytes, " + remaining + " left)", e);
            }
            remaining -= chunk;
        }
    }

    private VectorStream streamHdf5Vectors(long skipVectors, long maxVectors) {
        // Load all vectors from HDF5 into memory, then stream from the array.
        // HDF5 datasets are bounded by int array indices; the long signature is
        // for consistency with streamBaseVectors().
        HdfFile hdf = new HdfFile(getHdf5File().toPath());
        Dataset ds = hdf.getDatasetByPath("train");
        float[][] data = (float[][]) ds.getData();
        int start = (int) Math.min(skipVectors, data.length);
        int end = (int) Math.min((long) start + maxVectors, data.length);
        System.out.println("  Loaded " + data.length + " vectors from HDF5 into memory, streaming [" + start + ", " + end + ")");
        return new VectorStream() {
            @Override
            public void close() throws IOException {
                hdf.close();
            }

            @Override
            public Iterator<float[]> iterator() {
                return new Iterator<float[]>() {
                    private int idx = start;

                    @Override
                    public boolean hasNext() {
                        return idx < end;
                    }

                    @Override
                    public float[] next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        float[] v = data[idx++];
                        if ((idx - start) % 1_000_000 == 0) {
                            System.out.println("  Streamed " + (idx - start) + " vectors...");
                        }
                        return v;
                    }
                };
            }
        };
    }

    public List<float[]> loadQueryVectors(int maxVectors) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return loadHdf5Vectors("test", maxVectors);
        }
        String queryFileName = preset == DatasetPreset.CUSTOM && customDescriptor != null
                ? customDescriptor.queryFile : preset.queryFile;
        File file = new File(getDatasetSubDir(), queryFileName);
        VecFormat format = resolveQueryFormat();
        if (format == VecFormat.BVECS) {
            return loadBvecs(openInputStream(file), maxVectors);
        } else {
            return loadFvecs(openInputStream(file), maxVectors);
        }
    }

    public List<int[]> loadGroundTruth(int maxVectors) throws IOException {
        // Delegate to the count-aware variant. For non-CUSTOM presets the count is ignored;
        // for CUSTOM datasets we ask for the ground truth matching totalVectors (the legacy
        // file). Callers that want recall for a prefix run should call
        // loadGroundTruth(maxVectors, baseVectorCount) directly.
        long defaultCount = preset == DatasetPreset.CUSTOM && customDescriptor != null
                ? customDescriptor.totalVectors
                : Long.MIN_VALUE; // sentinel: ignored for non-CUSTOM presets
        return loadGroundTruth(maxVectors, defaultCount);
    }

    /**
     * Loads ground truth for a specific base-vector count. Used when a custom dataset
     * ships multiple ground-truth files keyed by prefix size (e.g. for prefix runs at
     * 1M, 10M, 1B base vectors).
     *
     * @param maxVectors      max ground-truth records to load (typically the query count).
     * @param baseVectorCount the number of base vectors the bench will run against.
     *                        For CUSTOM datasets, the descriptor's
     *                        {@code groundTruthCheckpoints} list must contain an entry
     *                        with this exact count, or an IOException is thrown.
     *                        Ignored for HDF5 and non-CUSTOM presets.
     * @throws IOException if the matching ground truth file is missing, or, for CUSTOM
     *                     datasets, no checkpoint matches {@code baseVectorCount}.
     */
    public List<int[]> loadGroundTruth(int maxVectors, long baseVectorCount) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return loadHdf5GroundTruth(maxVectors);
        }
        String gtFileName;
        if (preset == DatasetPreset.CUSTOM && customDescriptor != null) {
            gtFileName = resolveCustomGroundTruthFile(baseVectorCount);
        } else {
            gtFileName = preset.groundTruthFile;
        }
        File file = new File(getDatasetSubDir(), gtFileName);
        return loadIvecs(openInputStream(file), maxVectors);
    }

    /**
     * Picks the ground-truth file from {@link DatasetDescriptor#groundTruthCheckpoints}
     * matching {@code baseVectorCount} exactly. Recall against a non-matching ground
     * truth is mathematically meaningless, so we fail hard rather than silently use a
     * close-but-wrong file.
     */
    private String resolveCustomGroundTruthFile(long baseVectorCount) throws IOException {
        List<DatasetDescriptor.GroundTruthCheckpoint> checkpoints =
                customDescriptor.groundTruthCheckpoints;
        if (checkpoints == null || checkpoints.isEmpty()) {
            // Old descriptor with neither field present, or HDF5-style: fall back to the
            // legacy single-file field. If that's also null the IO read will fail with a
            // clear message.
            if (customDescriptor.groundTruthFile == null) {
                throw new IOException("Custom dataset has no ground truth file configured");
            }
            return customDescriptor.groundTruthFile;
        }
        for (DatasetDescriptor.GroundTruthCheckpoint cp : checkpoints) {
            if (cp.baseVectorCount == baseVectorCount) {
                return cp.file;
            }
        }
        StringBuilder available = new StringBuilder();
        for (int i = 0; i < checkpoints.size(); i++) {
            if (i > 0) {
                available.append(", ");
            }
            available.append(checkpoints.get(i).baseVectorCount);
        }
        throw new IOException("No ground truth checkpoint matches baseVectorCount=" + baseVectorCount
                + " for custom dataset; available checkpoints: [" + available + "]."
                + " Pass --rows N where N is one of the listed values to enable recall;"
                + " or omit --rows to use the full dataset.");
    }

    private List<float[]> loadFvecs(InputStream in, int maxVectors) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(in)) {
            // Issue #443: reuse the byte[] and FloatBuffer view across
            // every vector in this file. SIFT files have constant dim,
            // so a single allocation suffices.
            byte[] scratchBytes = null;
            FloatBuffer scratchView = null;
            int cachedDim = -1;
            while (vectors.size() < maxVectors) {
                int dim;
                try {
                    dim = readLittleEndianInt(dis);
                } catch (java.io.EOFException e) {
                    break;
                }
                if (scratchBytes == null || cachedDim != dim) {
                    cachedDim = dim;
                    scratchBytes = new byte[dim * 4];
                    scratchView = ByteBuffer.wrap(scratchBytes)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .asFloatBuffer();
                }
                dis.readFully(scratchBytes);
                float[] vec = new float[dim];
                scratchView.rewind();
                scratchView.get(vec);
                vectors.add(vec);
                if (vectors.size() % 1_000_000 == 0) {
                    System.out.println("  Loaded " + vectors.size() + " vectors...");
                }
            }
        }
        return vectors;
    }

    private List<float[]> loadBvecs(InputStream in, int maxVectors) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(in)) {
            // Issue #443: reuse the byte[] across every vector.
            byte[] scratchBytes = null;
            int cachedDim = -1;
            while (vectors.size() < maxVectors) {
                int dim;
                try {
                    dim = readLittleEndianInt(dis);
                } catch (java.io.EOFException e) {
                    break;
                }
                if (scratchBytes == null || cachedDim != dim) {
                    cachedDim = dim;
                    scratchBytes = new byte[dim];
                }
                dis.readFully(scratchBytes);
                float[] vec = new float[dim];
                for (int i = 0; i < dim; i++) {
                    vec[i] = (scratchBytes[i] & 0xFF);
                }
                vectors.add(vec);
                if (vectors.size() % 1_000_000 == 0) {
                    System.out.println("  Loaded " + vectors.size() + " vectors...");
                }
            }
        }
        return vectors;
    }

    private List<int[]> loadIvecs(InputStream in, int maxVectors) throws IOException {
        List<int[]> vectors = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(in)) {
            while (vectors.size() < maxVectors) {
                int dim;
                try {
                    dim = readLittleEndianInt(dis);
                } catch (java.io.EOFException e) {
                    break;
                }
                byte[] bytes = new byte[dim * 4];
                dis.readFully(bytes);
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                int[] vec = new int[dim];
                for (int i = 0; i < dim; i++) {
                    vec[i] = bb.getInt();
                }
                vectors.add(vec);
            }
        }
        return vectors;
    }

    private static int readLittleEndianInt(DataInputStream dis) throws IOException {
        byte[] bytes = new byte[4];
        dis.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    /**
     * Descriptor for a custom generated dataset (loaded from JSON).
     */
    static class DatasetDescriptor {
        final String name;
        final String similarity;
        final VecFormat format;
        final int dimensions;
        // Long-valued — datasets like BIGANN already exceed INT_MAX and future
        // work targets 20 B+ vector corpora.
        final long totalVectors;
        final long numQueries;
        final int groundTruthK;
        final String baseFile;
        final String queryFile;
        // Legacy single ground-truth file. Always points at the file matching
        // {@code totalVectors}. New descriptors duplicate this entry as the last
        // element of {@link #groundTruthCheckpoints}.
        final String groundTruthFile;
        // Ascending list of (baseVectorCount, file) pairs. For new descriptors the
        // last entry's file equals {@link #groundTruthFile} and its baseVectorCount
        // equals {@link #totalVectors}. For old descriptors that lack the
        // groundTruthCheckpoints array, this is synthesised from groundTruthFile +
        // totalVectors so callers see a uniform shape.
        final List<GroundTruthCheckpoint> groundTruthCheckpoints;

        DatasetDescriptor(String name, String similarity, VecFormat format,
                          int dimensions, long totalVectors,
                          long numQueries, int groundTruthK,
                          String baseFile, String queryFile, String groundTruthFile,
                          List<GroundTruthCheckpoint> groundTruthCheckpoints) {
            this.name = name;
            this.similarity = similarity;
            this.format = format;
            this.dimensions = dimensions;
            this.totalVectors = totalVectors;
            this.numQueries = numQueries;
            this.groundTruthK = groundTruthK;
            this.baseFile = baseFile;
            this.queryFile = queryFile;
            this.groundTruthFile = groundTruthFile;
            this.groundTruthCheckpoints = groundTruthCheckpoints == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(groundTruthCheckpoints));
        }

        /**
         * One ground-truth file in a multi-checkpoint custom dataset. {@code file} is a
         * relative path resolved against the dataset directory.
         */
        static final class GroundTruthCheckpoint {
            final long baseVectorCount;
            final String file;

            GroundTruthCheckpoint(long baseVectorCount, String file) {
                this.baseVectorCount = baseVectorCount;
                this.file = file;
            }
        }

        static DatasetDescriptor load(File jsonFile) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(jsonFile);
            String groundTruthFile = root.path("groundTruthFile").asText(null);
            long totalVectors = root.path("totalVectors").asLong(0L);
            List<GroundTruthCheckpoint> checkpoints = parseCheckpoints(
                    root.path("groundTruthCheckpoints"), totalVectors, groundTruthFile);
            return new DatasetDescriptor(
                    root.path("name").asText("custom"),
                    root.path("similarity").asText("euclidean"),
                    parseFormat(root.path("format").asText("fvecs")),
                    root.path("dimensions").asInt(0),
                    totalVectors,
                    root.path("numQueries").asLong(0L),
                    root.path("groundTruthK").asInt(0),
                    root.path("baseFile").asText(null),
                    root.path("queryFile").asText(null),
                    groundTruthFile,
                    checkpoints
            );
        }

        /**
         * Parses the {@code groundTruthCheckpoints} array. When the array is missing
         * or empty, synthesises a singleton from the legacy {@code groundTruthFile} +
         * {@code totalVectors} so old descriptors still resolve to a single ground-truth
         * file via the new lookup path.
         */
        private static List<GroundTruthCheckpoint> parseCheckpoints(
                JsonNode arrayNode, long totalVectors, String groundTruthFile) {
            List<GroundTruthCheckpoint> out = new ArrayList<>();
            if (arrayNode != null && arrayNode.isArray() && arrayNode.size() > 0) {
                for (JsonNode entry : arrayNode) {
                    long count = entry.path("baseVectorCount").asLong(-1L);
                    String file = entry.path("file").asText(null);
                    if (count <= 0 || file == null) {
                        continue;
                    }
                    out.add(new GroundTruthCheckpoint(count, file));
                }
            }
            if (out.isEmpty() && groundTruthFile != null && totalVectors > 0) {
                out.add(new GroundTruthCheckpoint(totalVectors, groundTruthFile));
            }
            return out;
        }

        private static VecFormat parseFormat(String s) throws IOException {
            if (s == null || s.isEmpty()) {
                return VecFormat.FVECS;
            }
            switch (s.toLowerCase(java.util.Locale.ROOT)) {
                case "fvecs":
                    return VecFormat.FVECS;
                case "bvecs":
                    return VecFormat.BVECS;
                default:
                    throw new IOException("Unsupported descriptor format: " + s
                            + " (expected fvecs or bvecs)");
            }
        }
    }

    private DatasetDescriptor customDescriptor;

    /**
     * Load and configure from a descriptor JSON file (for custom generated datasets).
     * Call this after construction with CUSTOM preset to populate file paths and similarity.
     */
    public DatasetDescriptor loadDescriptor() throws IOException {
        File dir = getDatasetSubDir();
        // Find descriptor JSON in the dataset directory
        File[] descriptors = dir.listFiles((d, name) -> name.endsWith("_descriptor.json"));
        if (descriptors == null || descriptors.length == 0) {
            throw new IOException("No descriptor JSON found in " + dir.getAbsolutePath());
        }
        customDescriptor = DatasetDescriptor.load(descriptors[0]);
        System.out.println("Loaded dataset descriptor: " + descriptors[0].getName());
        System.out.println("  Name: " + customDescriptor.name
                + ", Vectors: " + customDescriptor.totalVectors
                + ", Dimensions: " + customDescriptor.dimensions
                + ", Similarity: " + customDescriptor.similarity
                + ", Format: " + customDescriptor.format
                + ", Queries: " + customDescriptor.numQueries
                + ", GroundTruth-K: " + customDescriptor.groundTruthK);
        if (!customDescriptor.groundTruthCheckpoints.isEmpty()) {
            StringBuilder cps = new StringBuilder();
            for (int i = 0; i < customDescriptor.groundTruthCheckpoints.size(); i++) {
                if (i > 0) {
                    cps.append(", ");
                }
                cps.append(customDescriptor.groundTruthCheckpoints.get(i).baseVectorCount);
            }
            System.out.println("  Ground-truth checkpoints (base-vector counts): [" + cps + "]");
        }
        return customDescriptor;
    }

    public DatasetDescriptor getCustomDescriptor() {
        return customDescriptor;
    }

    enum VecFormat { FVECS, BVECS, HDF5 }

    enum DatasetPreset {
        SIFT10K(
                "siftsmall",
                "ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz",
                "siftsmall_base.fvecs", VecFormat.FVECS,
                "siftsmall_query.fvecs", VecFormat.FVECS,
                "siftsmall_groundtruth.ivecs",
                "euclidean"
        ),
        SIFT1M(
                "sift",
                "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz",
                "sift_base.fvecs", VecFormat.FVECS,
                "sift_query.fvecs", VecFormat.FVECS,
                "sift_groundtruth.ivecs",
                "euclidean"
        ),
        SIFT10M(
                "bigann",
                "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz",
                "bigann_base.bvecs", VecFormat.BVECS,
                "bigann_query.bvecs", VecFormat.BVECS,
                "bigann_gnd/idx_10000000.ivecs",
                "euclidean"
        ),
        GIST1M(
                "gist",
                "ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz",
                "gist_base.fvecs", VecFormat.FVECS,
                "gist_query.fvecs", VecFormat.FVECS,
                "gist_groundtruth.ivecs",
                "euclidean"
        ),
        BIGANN(
                "bigann",
                "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz",
                "bigann_base.bvecs", VecFormat.BVECS,
                "bigann_query.bvecs", VecFormat.BVECS,
                "bigann_gnd/idx_1000000000.ivecs",
                "euclidean"
        ),
        GLOVE_100(
                "glove-100",
                "http://ann-benchmarks.com/glove-100-angular.hdf5",
                "glove-100-angular.hdf5", VecFormat.HDF5,
                null, VecFormat.HDF5,
                null,
                "cosine"
        ),
        DEEP_IMAGE_96(
                "deep-image-96",
                "http://ann-benchmarks.com/deep-image-96-angular.hdf5",
                "deep-image-96-angular.hdf5", VecFormat.HDF5,
                null, VecFormat.HDF5,
                null,
                "cosine"
        ),
        CUSTOM(
                "custom",
                null,
                null, VecFormat.FVECS,
                null, VecFormat.FVECS,
                null,
                "euclidean"
        );

        final String subDir;
        final String defaultUrl;
        final String baseFile;
        final VecFormat baseFormat;
        final String queryFile;
        final VecFormat queryFormat;
        final String groundTruthFile;
        final String similarity;

        DatasetPreset(String subDir, String defaultUrl,
                      String baseFile, VecFormat baseFormat,
                      String queryFile, VecFormat queryFormat,
                      String groundTruthFile, String similarity) {
            this.subDir = subDir;
            this.defaultUrl = defaultUrl;
            this.baseFile = baseFile;
            this.baseFormat = baseFormat;
            this.queryFile = queryFile;
            this.queryFormat = queryFormat;
            this.groundTruthFile = groundTruthFile;
            this.similarity = similarity;
        }
    }
}
