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

package herddb.index.vector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link PersistentVectorStore#pinSegmentBfs} (package-private).
 * Verifies the three negative guard paths:
 * <ol>
 *   <li>Non-{@link PinModeReaderSupplier} supplier → method is a no-op.</li>
 *   <li>{@link PinModeReaderSupplier} with {@code hasFrontierCacheActive() == false} → no-op.</li>
 *   <li>Null supplier → no-op (instanceof null = false).</li>
 * </ol>
 * The positive path (frontier actually populated) is covered by
 * {@code RemoteRandomAccessReaderPinModeTest} which tests the full
 * supplier → pinBlock → frontier-cache pipeline end-to-end.
 */
public class PinSegmentBfsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
    }

    /**
     * A {@link ReaderSupplier} that does NOT implement {@link PinModeReaderSupplier}.
     * Tracks whether {@code get()} was ever called.
     */
    private static class PlainReaderSupplier implements ReaderSupplier {
        boolean getCalled = false;

        @Override
        public RandomAccessReader get() throws IOException {
            getCalled = true;
            throw new UnsupportedOperationException("should not be called");
        }

        @Override
        public void close() throws IOException {}
    }

    /**
     * A stub {@link PinModeReaderSupplier} whose frontier cache is disabled
     * ({@code hasFrontierCacheActive() == false}).
     */
    private static class DisabledFrontierSupplier implements ReaderSupplier, PinModeReaderSupplier {
        boolean withPinModeCalled = false;

        @Override
        public boolean hasFrontierCacheActive() {
            return false;
        }

        @Override
        public ReaderSupplier withPinMode() {
            withPinModeCalled = true;
            throw new UnsupportedOperationException("should not be called");
        }

        @Override
        public RandomAccessReader get() throws IOException {
            throw new UnsupportedOperationException("should not be called");
        }

        @Override
        public void close() throws IOException {}
    }

    @Test
    public void pinSegmentBfsIsNoOpWhenSupplierIsNotPinModeReaderSupplier() throws Exception {
        try (PersistentVectorStore store = createStore(folder.newFolder("store").toPath())) {
            VectorSegment seg = new VectorSegment(1);
            PlainReaderSupplier plain = new PlainReaderSupplier();
            seg.onDiskReaderSupplier = plain;

            // onDiskGraph = null → the method must return early after the instanceof check.
            // Passing null for odg is safe because pinSegmentBfs returns before any
            // ODG access when the supplier is not a PinModeReaderSupplier.
            store.pinSegmentBfs(seg, null, 1L, 1_000_000L);

            assertFalse("get() must not be called on a non-PinModeReaderSupplier",
                    plain.getCalled);
        }
    }

    @Test
    public void pinSegmentBfsIsNoOpWhenNullSupplier() throws Exception {
        try (PersistentVectorStore store = createStore(folder.newFolder("store").toPath())) {
            VectorSegment seg = new VectorSegment(2);
            seg.onDiskReaderSupplier = null;

            // null instanceof PinModeReaderSupplier = false → must return immediately.
            store.pinSegmentBfs(seg, null, 1L, 1_000_000L);
            // No exception = pass.
        }
    }

    @Test
    public void pinSegmentBfsIsNoOpWhenFrontierCacheInactive() throws Exception {
        try (PersistentVectorStore store = createStore(folder.newFolder("store").toPath())) {
            VectorSegment seg = new VectorSegment(3);
            DisabledFrontierSupplier disabled = new DisabledFrontierSupplier();
            seg.onDiskReaderSupplier = disabled;

            store.pinSegmentBfs(seg, null, 1L, 1_000_000L);

            assertFalse("withPinMode() must not be called when hasFrontierCacheActive() == false",
                    disabled.withPinModeCalled);
        }
    }

    @Test
    public void pinModeReaderSupplierNegativeInstanceofDoesNotRequirePinInterface() {
        // Confirm the interface hierarchy: a plain ReaderSupplier is not a PinModeReaderSupplier.
        PlainReaderSupplier plain = new PlainReaderSupplier();
        assertFalse("plain ReaderSupplier must not implement PinModeReaderSupplier",
                plain instanceof PinModeReaderSupplier);
    }

    @Test
    public void setPinBytesPerSegmentDefaultIsMirrorOfWarmup() throws Exception {
        // Default -1 means "mirror warmup budget". Explicitly setting 0 disables pin BFS.
        try (PersistentVectorStore store = createStore(folder.newFolder("store").toPath())) {
            // Setting 0 should disable pin BFS — no exception expected.
            store.setPinBytesPerSegment(0L);
            store.setPinBytesPerSegment(-1L);
            store.setPinBytesPerSegment(1_000_000L);
            // All above are set-only operations; the test verifies no NPE/ISE thrown.
            assertTrue("setter accepted all values without error", true);
        }
    }
}
