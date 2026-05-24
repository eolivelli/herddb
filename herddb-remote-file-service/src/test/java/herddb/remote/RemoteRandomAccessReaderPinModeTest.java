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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.index.vector.PinModeReaderSupplier;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import java.util.List;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that {@link RemoteRandomAccessReader.Supplier} correctly implements
 * {@link PinModeReaderSupplier}: that {@link RemoteRandomAccessReader.Supplier#withPinMode()}
 * produces readers whose block loads are routed through
 * {@link SegmentBlockCache#pinBlock} (incrementing the frontier stats), and that
 * normal (non-pin-mode) readers use the main cache path instead.
 */
public class RemoteRandomAccessReaderPinModeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    private static final int BLOCK_SIZE = 64;
    private static final String PATH = "ts/idx/graph";

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));

        // Upload a small file so reads can succeed.
        byte[] data = new byte[BLOCK_SIZE * 4];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        client.writeFile(PATH, data);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    @Test
    public void supplierImplementsPinModeReaderSupplier() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        assertTrue("Supplier must implement PinModeReaderSupplier",
                supplier instanceof PinModeReaderSupplier);
    }

    @Test
    public void hasFrontierCacheActiveTrueWhenFrontierBudgetPositive() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        assertTrue("hasFrontierCacheActive must be true when frontier budget > 0",
                supplier.hasFrontierCacheActive());
    }

    @Test
    public void hasFrontierCacheActiveFalseWhenFrontierDisabled() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 0);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        assertFalse("hasFrontierCacheActive must be false when no frontier budget",
                supplier.hasFrontierCacheActive());
    }

    @Test
    public void pinModeReaderRoutesThroughPinBlock() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        // Read using a pin-mode reader.
        try (RandomAccessReader pinReader = supplier.withPinMode().get()) {
            pinReader.seek(0);
            pinReader.readInt(); // triggers ensureBlockLoaded → pinBlock
        }

        assertEquals("pin-mode reader must route loads through pinBlock (frontier load_success)",
                1L, cache.frontierLoadSuccessCount());
        assertEquals("main cache must not record a load (frontier only)",
                0L, cache.loadSuccessCount());
    }

    @Test
    public void normalReaderRoutesThoughMainCache() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        // Read using a normal (non-pin) reader.
        try (RandomAccessReader reader = supplier.get()) {
            reader.seek(0);
            reader.readInt(); // triggers ensureBlockLoaded → getBlock
        }

        assertEquals("normal reader must route loads through main cache (main load_success)",
                1L, cache.loadSuccessCount());
        assertEquals("frontier must not see any load from a normal reader",
                0L, cache.frontierLoadSuccessCount());
    }

    @Test
    public void pinModeHitCountedOnSubsequentGetBlock() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        // Prime the frontier region via a pin-mode read.
        try (RandomAccessReader pinReader = supplier.withPinMode().get()) {
            pinReader.seek(0);
            pinReader.readInt();
        }
        assertEquals(1L, cache.frontierLoadSuccessCount());

        // A subsequent normal read for the same block must come from the frontier.
        try (RandomAccessReader reader = supplier.get()) {
            reader.seek(0);
            reader.readInt();
        }

        assertEquals("normal reader must get a frontier hit for a previously pinned block",
                1L, cache.frontierHitCount());
        assertEquals("main cache must not record a separate load",
                0L, cache.loadSuccessCount());
    }

    @Test
    public void withPinModeReturnedSupplierIsNotSameInstance() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        RemoteRandomAccessReader.Supplier supplier =
                new RemoteRandomAccessReader.Supplier(client, PATH, BLOCK_SIZE * 4,
                        BLOCK_SIZE, BLOCK_SIZE, NullStatsLogger.INSTANCE, cache);

        RemoteRandomAccessReader.Supplier pinSupplier = supplier.withPinMode();
        assertFalse("withPinMode() must return a new instance, not this",
                supplier == pinSupplier);
        // Both should share the same frontier cache.
        assertTrue(pinSupplier.hasFrontierCacheActive());
    }
}
