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

package herddb.indexing;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class S3WatermarkStoreTest {

    /** In-memory fake of the remote file service for unit tests. */
    static class FakeRemoteFileIO implements S3WatermarkStore.RemoteFileIO {
        final Map<String, byte[]> store = new HashMap<>();
        volatile boolean failNextWrite = false;
        volatile boolean failNextRead = false;
        volatile int writeCount = 0;

        @Override
        public void writeFile(String path, byte[] content) throws IOException {
            if (failNextWrite) {
                failNextWrite = false;
                throw new IOException("injected write failure for " + path);
            }
            store.put(path, content.clone());
            writeCount++;
        }

        @Override
        public byte[] readFile(String path) throws IOException {
            if (failNextRead) {
                failNextRead = false;
                throw new IOException("injected read failure for " + path);
            }
            byte[] data = store.get(path);
            return data == null ? null : data.clone();
        }
    }

    @Test
    public void loadReturnsStartOfTimeWhenAbsent() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        assertEquals(LogSequenceNumber.START_OF_TIME, store.load());
    }

    @Test
    public void saveAndLoadRoundTrip() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        LogSequenceNumber saved = new LogSequenceNumber(42L, 7L);
        store.save(saved);

        LogSequenceNumber loaded = store.load();
        assertEquals(saved.ledgerId, loaded.ledgerId);
        assertEquals(saved.offset, loaded.offset);
    }

    @Test
    public void pathIncludesTablespaceAndInstance() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid-abc", 3);
        assertEquals("ts-uuid-abc/_indexing/3/watermark.lsn", store.getPath());
    }

    @Test
    public void monotonicSaveBlocksRegression() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        store.save(new LogSequenceNumber(5L, 100L));
        byte[] persisted = io.store.get(store.getPath());

        // Attempt to save an older LSN — should be a no-op.
        store.save(new LogSequenceNumber(5L, 50L));
        assertArrayEquals("older save must not overwrite",
                persisted, io.store.get(store.getPath()));

        // Verify the stored LSN is still the newer one.
        LogSequenceNumber loaded = store.load();
        assertEquals(5L, loaded.ledgerId);
        assertEquals(100L, loaded.offset);

        // A newer save goes through.
        store.save(new LogSequenceNumber(5L, 200L));
        loaded = store.load();
        assertEquals(200L, loaded.offset);
    }

    @Test
    public void equalLsnSaveIsIdempotent() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        LogSequenceNumber lsn = new LogSequenceNumber(1L, 1L);
        store.save(lsn);
        store.save(lsn);
        // Both saves happened — monotonicity allows equal LSN.
        assertEquals(2, io.writeCount);
        assertEquals(lsn.offset, store.load().offset);
    }

    @Test
    public void corruptObjectIsDetected() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);

        store.save(new LogSequenceNumber(7L, 99L));
        byte[] data = io.store.get(store.getPath());
        // Flip a byte in the payload (not the XXHash footer) — integrity check must catch.
        data[0] ^= 0x55;
        io.store.put(store.getPath(), data);

        assertThrows(S3WatermarkStore.CorruptWatermarkException.class, store::load);
    }

    @Test
    public void truncatedObjectIsDetected() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.store.put("ts-uuid/_indexing/0/watermark.lsn", new byte[]{1, 2, 3});
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        assertThrows(S3WatermarkStore.CorruptWatermarkException.class, store::load);
    }

    @Test
    public void saveAfterCorruptionOverwrites() throws IOException {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        // Seed a corrupt object.
        io.store.put(store.getPath(), new byte[]{9, 9, 9});
        // Writing over it must succeed (corrupt current value does not block saves).
        store.save(new LogSequenceNumber(3L, 3L));
        assertEquals(3L, store.load().offset);
    }

    @Test
    public void readFailurePropagatesAsIOException() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.failNextRead = true;
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        IOException ex = assertThrows(IOException.class, store::load);
        assertNotNull(ex.getMessage());
    }

    @Test
    public void writeFailurePropagatesAsIOException() {
        FakeRemoteFileIO io = new FakeRemoteFileIO();
        io.failNextWrite = true;
        S3WatermarkStore store = new S3WatermarkStore(io, "ts-uuid", 0);
        IOException ex = assertThrows(IOException.class,
                () -> store.save(new LogSequenceNumber(1L, 1L)));
        assertTrue(ex.getMessage().contains("watermark"));
        // No object was written.
        assertNull(io.store.get(store.getPath()));
    }
}
