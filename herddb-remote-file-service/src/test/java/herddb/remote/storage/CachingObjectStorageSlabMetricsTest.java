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

package herddb.remote.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.CachingObjectStorageTest.FakeObjectStorage;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the slab/fallback introspection methods on
 * {@link CachingObjectStorage} report values matching what the Prometheus
 * gauges in {@code RemoteFileServer} will sample (issue #475). Anything that
 * makes a gauge regress would cause the Grafana dashboard to display stale
 * data, so we hammer the lifecycle here.
 *
 * @author enrico.olivelli
 */
public class CachingObjectStorageSlabMetricsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    private CachingObjectStorage build(FakeObjectStorage inner, long maxBytes,
                                       CachingObjectStorage.SlabConfig slabConfig) throws Exception {
        Path cacheDir = folder.newFolder().toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, maxBytes, slabConfig);
    }

    @Test
    public void testSlabAndFallbackStatsLifecycle() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        // Small: 1 KiB cells x 2 cells; Large: 16 KiB cells x 2 cells; budget 1 MiB.
        CachingObjectStorage.SlabConfig cfg =
                new CachingObjectStorage.SlabConfig(true, 1024, 2 * 1024, 16 * 1024, 32 * 1024);
        try (CachingObjectStorage caching = build(inner, 1024 * 1024, cfg)) {
            SlabCacheFileStore small = caching.getSmallSlab();
            SlabCacheFileStore large = caching.getLargeSlab();
            assertNotNull(small);
            assertNotNull(large);

            // Initial state.
            assertEquals(1024, small.cellSize());
            assertEquals(2, small.totalCells());
            assertEquals(2, small.freeCells());
            assertEquals(0, small.liveCells());
            assertEquals(0, small.bytesInUse());
            assertEquals(0, small.admitCount());
            assertEquals(0, small.admitFullCount());
            assertEquals(0, small.evictCount());
            assertEquals(0L, caching.getFallbackFileCount());
            assertEquals(0L, caching.getFallbackBytes());

            // Admit one small.
            caching.write("a", new byte[800]).get();
            assertEquals(1, small.liveCells());
            assertEquals(1, small.freeCells());
            assertEquals(800, small.bytesInUse());
            assertEquals(1, small.admitCount());
            assertEquals(0, small.admitFullCount());

            // Admit one large.
            caching.write("b", new byte[8 * 1024]).get();
            assertEquals(1, large.liveCells());
            assertEquals(8 * 1024, large.bytesInUse());

            // Admit one fallback.
            caching.write("c", new byte[64 * 1024]).get();
            assertEquals(1L, caching.getFallbackFileCount());
            assertEquals(64L * 1024, caching.getFallbackBytes());

            // Saturate the small tier so a third small admit falls through.
            caching.write("a2", new byte[800]).get();
            assertEquals(2, small.liveCells());
            assertEquals(0, small.freeCells());
            // Force a small-tier-full admit: this will fall through to the large tier
            // (since 800 ≤ largeCell). admit_full_count on small must increment.
            long smallFullBefore = small.admitFullCount();
            caching.write("a3", new byte[800]).get();
            assertEquals("small admit_full_count must increment on tier-full",
                    smallFullBefore + 1, small.admitFullCount());
            assertEquals(2, large.liveCells());

            // Evict everything via delete.
            caching.delete("a").get();
            caching.delete("a2").get();
            caching.delete("a3").get();
            caching.delete("b").get();
            caching.delete("c").get();

            // After cleanup, slab counters must be zero and fallback gauges drained.
            assertEquals(0, small.liveCells());
            assertEquals(2, small.freeCells());
            assertEquals(0, small.bytesInUse());
            assertEquals(0, large.liveCells());
            assertEquals(2, large.freeCells());
            assertEquals(0, large.bytesInUse());
            assertEquals(0L, caching.getFallbackFileCount());
            assertEquals(0L, caching.getFallbackBytes());

            // Evict counters must be monotonic and reflect the actual evicts. Two
            // small entries (a, a2), two large entries (b plus the fall-through admit
            // of a3) — so we expect at least 2 small + 2 large evicts.
            assertTrue("small.evictCount() must reflect deletes; got " + small.evictCount(),
                    small.evictCount() >= 2);
            assertTrue("large.evictCount() must reflect deletes; got " + large.evictCount(),
                    large.evictCount() >= 2);
        }
    }

    @Test
    public void testSlabDisabledReportsNullSlabsAndZeroSlabStats() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        try (CachingObjectStorage caching = build(inner, 64 * 1024,
                CachingObjectStorage.SlabConfig.disabled())) {
            assertNull("slab disabled => no small slab", caching.getSmallSlab());
            assertNull("slab disabled => no large slab", caching.getLargeSlab());
            // Fallback counters must still work.
            caching.write("a", new byte[100]).get();
            caching.write("b", new byte[200]).get();
            assertEquals(2L, caching.getFallbackFileCount());
            assertEquals(300L, caching.getFallbackBytes());
        }
    }
}
