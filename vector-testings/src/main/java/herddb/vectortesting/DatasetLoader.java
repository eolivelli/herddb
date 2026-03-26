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

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.time.Duration;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

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
        // For HDF5 datasets, query and ground truth are inside the same file
        if (preset.baseFormat == VecFormat.HDF5) {
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
            try { totalFileSize = Long.parseLong(sizeReply.trim()); } catch (NumberFormatException ignored) {}
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
        File file = new File(getDatasetSubDir(), preset.baseFile);
        if (preset.baseFormat == VecFormat.BVECS) {
            return loadBvecs(openInputStream(file), maxVectors);
        } else {
            return loadFvecs(openInputStream(file), maxVectors);
        }
    }

    public interface VectorStream extends Iterable<float[]>, Closeable {}

    public VectorStream streamBaseVectors(int maxVectors) throws IOException {
        return streamBaseVectors(0, maxVectors);
    }

    /**
     * Stream up to {@code maxVectors} base vectors, skipping the first {@code skipVectors}.
     * Row IDs should start from {@code skipVectors} when resuming ingestion.
     */
    public VectorStream streamBaseVectors(int skipVectors, int maxVectors) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return streamHdf5Vectors(skipVectors, maxVectors);
        }
        File file = new File(getDatasetSubDir(), preset.baseFile);
        InputStream in = openInputStream(file);
        DataInputStream dis = new DataInputStream(in);
        VecFormat format = preset.baseFormat;

        if (skipVectors > 0) {
            System.out.println("  Skipping " + skipVectors + " vectors to resume from position " + skipVectors + "...");
            skipVecsInStream(dis, format, skipVectors);
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
                    private int count = 0;
                    private float[] next = null;
                    private boolean eof = false;

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
                            if (format == VecFormat.BVECS) {
                                byte[] bytes = new byte[dim];
                                dis.readFully(bytes);
                                float[] vec = new float[dim];
                                for (int i = 0; i < dim; i++) {
                                    vec[i] = (bytes[i] & 0xFF);
                                }
                                return vec;
                            } else {
                                byte[] bytes = new byte[dim * 4];
                                dis.readFully(bytes);
                                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                                float[] vec = new float[dim];
                                for (int i = 0; i < dim; i++) {
                                    vec[i] = bb.getFloat();
                                }
                                return vec;
                            }
                        } catch (IOException e) {
                            eof = true;
                            throw new RuntimeException("Error reading vector stream", e);
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

    /** Skip {@code count} vectors in-place from a DataInputStream of FVECS or BVECS format. */
    private static void skipVecsInStream(DataInputStream dis, VecFormat format, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            int dim = readLittleEndianInt(dis);
            long bytesToSkip = (format == VecFormat.BVECS) ? dim : (long) dim * 4;
            long remaining = bytesToSkip;
            while (remaining > 0) {
                long skipped = dis.skip(remaining);
                if (skipped <= 0) {
                    throw new IOException("Unexpected end of stream while skipping vector " + i);
                }
                remaining -= skipped;
            }
            if ((i + 1) % 1_000_000 == 0) {
                System.out.println("  Skipped " + (i + 1) + " vectors...");
            }
        }
    }

    private VectorStream streamHdf5Vectors(int maxVectors) {
        return streamHdf5Vectors(0, maxVectors);
    }

    private VectorStream streamHdf5Vectors(int skipVectors, int maxVectors) {
        // Load all vectors from HDF5 into memory, then stream from the array
        HdfFile hdf = new HdfFile(getHdf5File().toPath());
        Dataset ds = hdf.getDatasetByPath("train");
        float[][] data = (float[][]) ds.getData();
        int start = Math.min(skipVectors, data.length);
        int end = Math.min(start + maxVectors, data.length);
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
        File file = new File(getDatasetSubDir(), preset.queryFile);
        if (preset.queryFormat == VecFormat.BVECS) {
            return loadBvecs(openInputStream(file), maxVectors);
        } else {
            return loadFvecs(openInputStream(file), maxVectors);
        }
    }

    public List<int[]> loadGroundTruth(int maxVectors) throws IOException {
        if (preset.baseFormat == VecFormat.HDF5) {
            return loadHdf5GroundTruth(maxVectors);
        }
        File file = new File(getDatasetSubDir(), preset.groundTruthFile);
        return loadIvecs(openInputStream(file), maxVectors);
    }

    private List<float[]> loadFvecs(InputStream in, int maxVectors) throws IOException {
        List<float[]> vectors = new ArrayList<>();
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
                float[] vec = new float[dim];
                for (int i = 0; i < dim; i++) {
                    vec[i] = bb.getFloat();
                }
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
            while (vectors.size() < maxVectors) {
                int dim;
                try {
                    dim = readLittleEndianInt(dis);
                } catch (java.io.EOFException e) {
                    break;
                }
                byte[] bytes = new byte[dim];
                dis.readFully(bytes);
                float[] vec = new float[dim];
                for (int i = 0; i < dim; i++) {
                    vec[i] = (bytes[i] & 0xFF);
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
