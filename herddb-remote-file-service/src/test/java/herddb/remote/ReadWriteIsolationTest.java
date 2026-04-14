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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test for issue #100: separate read/write execution lanes.
 *
 * <p>The test starts a {@link RemoteFileServer} with small, dedicated
 * read/write executor pools, pre-writes a multipart file, then measures
 * read-path latency in two phases:
 * <ol>
 *   <li>baseline reads, no writes in flight</li>
 *   <li>contention reads concurrent with a sustained stream of
 *       {@code writeFileBlockAsync} uploads</li>
 * </ol>
 *
 * <p>Under the lane split the read path must survive the contention phase
 * without timing out. We assert:
 * <ul>
 *   <li>zero read-path failures during contention,</li>
 *   <li>contention-phase p99 read latency stays within a reasonable
 *       multiple of the baseline (protects against regressions that
 *       re-couple the two lanes).</li>
 * </ul>
 */
public class ReadWriteIsolationTest {

    private static final int BLOCK_SIZE = 64 * 1024;         // 64 KB
    private static final int FILE_BLOCKS = 128;              // 8 MB target file
    private static final int READ_LENGTH = 4096;             // 4 KB per read
    private static final int BASELINE_READ_COUNT = 200;
    private static final int CONTENTION_READ_COUNT = 200;
    private static final long CONTENTION_WINDOW_MS = 10_000;
    private static final long READ_P99_CEILING_MS = 2_000;

    private static final String TARGET_PATH = "ts1/isolation/graph.bin";
    private static final String WRITE_NOISE_PATH = "ts1/isolation/noise.bin";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty(RemoteFileServer.CONFIG_READ_EXECUTOR_THREADS, "4");
        props.setProperty(RemoteFileServer.CONFIG_WRITE_EXECUTOR_THREADS, "4");
        props.setProperty("block.size", String.valueOf(BLOCK_SIZE));
        server = new RemoteFileServer("localhost", 0, folder.newFolder("data").toPath(), 2, props);
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));

        // Pre-write a multipart target file so the read phase has something
        // to fetch. Written block-by-block via writeFileBlockAsync so it
        // lands on the server in the same layout we'll read from.
        byte[] block = new byte[BLOCK_SIZE];
        for (int i = 0; i < FILE_BLOCKS; i++) {
            for (int j = 0; j < block.length; j++) {
                block[j] = (byte) ((i + j) & 0xff);
            }
            client.writeFileBlockAsync(TARGET_PATH, i, block).get(30, TimeUnit.SECONDS);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void readsNotStarvedByConcurrentWrites() throws Exception {
        long[] baseline = runReadWorkload(BASELINE_READ_COUNT);
        long baselineP99 = percentile(baseline, 0.99);

        AtomicBoolean stopWrites = new AtomicBoolean(false);
        AtomicInteger writeCount = new AtomicInteger();
        AtomicInteger writeErrors = new AtomicInteger();
        byte[] writeBlock = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(writeBlock);

        Thread writer = new Thread(() -> {
            long nextBlock = 0;
            while (!stopWrites.get()) {
                try {
                    CompletableFuture<Void> f =
                            client.writeFileBlockAsync(WRITE_NOISE_PATH, nextBlock, writeBlock);
                    f.get(30, TimeUnit.SECONDS);
                    writeCount.incrementAndGet();
                    nextBlock++;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (java.util.concurrent.ExecutionException
                        | java.util.concurrent.TimeoutException ex) {
                    writeErrors.incrementAndGet();
                }
            }
        }, "write-noise");
        writer.setDaemon(true);
        writer.start();

        // Give the writer a head start so its I/O is actually in flight when
        // the read workload begins — otherwise the baseline and contention
        // phases are indistinguishable.
        Thread.sleep(500);

        long contentionStart = System.currentTimeMillis();
        long[] contention = runReadWorkload(CONTENTION_READ_COUNT);
        long contentionElapsed = System.currentTimeMillis() - contentionStart;

        // Keep the writer going for the full window so every read overlaps.
        long remaining = CONTENTION_WINDOW_MS - contentionElapsed;
        if (remaining > 0) {
            Thread.sleep(remaining);
        }
        stopWrites.set(true);
        writer.join(TimeUnit.SECONDS.toMillis(60));

        long contentionP99 = percentile(contention, 0.99);

        assertTrue("writer must have driven some writes to exercise the write lane, got "
                + writeCount.get(), writeCount.get() > 0);
        assertEquals("writer saw errors: " + writeErrors.get(), 0, writeErrors.get());
        // Every read must complete successfully — no timeouts, no exceptions.
        // (runReadWorkload throws on any failure, so reaching this point already
        // proves it; the assertion below guards against future silent-failure refactors.)
        assertEquals(CONTENTION_READ_COUNT, contention.length);

        long ceiling = Math.max(4 * baselineP99, TimeUnit.MILLISECONDS.toNanos(READ_P99_CEILING_MS));
        assertTrue(
                "read p99 under contention regressed: baseline_p99=" + (baselineP99 / 1_000_000) + "ms"
                        + ", contention_p99=" + (contentionP99 / 1_000_000) + "ms"
                        + ", ceiling=" + (ceiling / 1_000_000) + "ms"
                        + ", writes_completed=" + writeCount.get(),
                contentionP99 <= ceiling);
    }

    private long[] runReadWorkload(int n) throws Exception {
        long[] latencies = new long[n];
        Random rng = new Random(12345);
        for (int i = 0; i < n; i++) {
            long offset = (long) rng.nextInt(FILE_BLOCKS * BLOCK_SIZE - READ_LENGTH);
            long t0 = System.nanoTime();
            byte[] data = client.readFileRange(TARGET_PATH, offset, READ_LENGTH, BLOCK_SIZE);
            long t1 = System.nanoTime();
            assertNotNull("read returned null at offset " + offset, data);
            assertEquals("read returned short data at offset " + offset, READ_LENGTH, data.length);
            latencies[i] = t1 - t0;
        }
        return latencies;
    }

    private static long percentile(long[] samples, double p) {
        long[] sorted = samples.clone();
        java.util.Arrays.sort(sorted);
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        if (idx < 0) {
            idx = 0;
        }
        if (idx >= sorted.length) {
            idx = sorted.length - 1;
        }
        return sorted[idx];
    }

}
