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

package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Unit tests for {@link PagedPkSet} — the BLink-backed PK set used by
 * {@link PersistentVectorStore} for {@code pendingCheckpointDeletes} and
 * {@code pendingCompactionDeletes} (issue #290).
 */
public class PagedPkSetTest {

    private static Bytes pk(int i) {
        return Bytes.from_array(("pk_" + i).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void addAndContainsAreConsistent() throws IOException {
        try (PagedPkSet set = TestBLinks.inMemoryPagedPkSet()) {
            assertFalse(set.contains(pk(1)));
            set.add(pk(1));
            assertTrue(set.contains(pk(1)));
            assertFalse(set.contains(pk(2)));
            set.add(pk(2));
            assertTrue(set.contains(pk(2)));
            assertEquals(2L, set.size());
        }
    }

    @Test
    public void addIsIdempotent() throws IOException {
        try (PagedPkSet set = TestBLinks.inMemoryPagedPkSet()) {
            set.add(pk(7));
            set.add(pk(7));
            set.add(pk(7));
            assertTrue(set.contains(pk(7)));
            assertEquals("duplicate adds produce a single entry",
                    1L, set.size());
        }
    }

    @Test
    public void forEachVisitsEveryPk() throws IOException {
        try (PagedPkSet set = TestBLinks.inMemoryPagedPkSet()) {
            for (int i = 0; i < 100; i++) {
                set.add(pk(i));
            }
            Set<Bytes> visited = new HashSet<>();
            set.forEach(visited::add);
            assertEquals(100, visited.size());
            for (int i = 0; i < 100; i++) {
                assertTrue("PK pk_" + i + " visited", visited.contains(pk(i)));
            }
        }
    }

    @Test
    public void closeFiresOnDropExactlyOnce() throws IOException {
        int[] count = new int[1];
        herddb.index.blink.BLink<Bytes, Long> blink = TestBLinks.inMemoryPkToNode();
        try (PagedPkSet set = new PagedPkSet(blink, () -> count[0]++)) {
            set.add(pk(1));
        }
        assertEquals(1, count[0]);
    }

    @Test
    public void largeSetWithSpillStillCorrect() throws IOException {
        // Force the BLink to spill: small page size + tiny resident-page
        // capacity. Adding 5 000 PKs ensures multiple page evictions.
        try (PagedPkSet set = new PagedPkSet(
                newSpillyBLink(), () -> { })) {
            for (int i = 0; i < 5000; i++) {
                set.add(pk(i));
            }
            assertEquals(5000L, set.size());
            for (int i = 0; i < 5000; i += 53) {
                assertTrue("pk_" + i, set.contains(pk(i)));
            }
            assertFalse(set.contains(pk(99999)));
        }
    }

    private static herddb.index.blink.BLink<Bytes, Long> newSpillyBLink() {
        // Tiny page-replacement budget — 4 resident pages × 1 KiB each.
        return new herddb.index.blink.BLink<>(
                /*pageSize*/ 1024L,
                herddb.index.blink.BytesLongSizeEvaluator.INSTANCE,
                new herddb.core.RandomPageReplacementPolicy(4),
                new TestInMemoryBytesLongStorage());
    }

    /**
     * Minimal in-memory BLinkIndexDataStorage implementation copy for the
     * spill test — kept here to avoid making
     * {@link TestBLinks}'s private impl visible.
     */
    private static final class TestInMemoryBytesLongStorage implements
            herddb.index.blink.BLinkIndexDataStorage<Bytes, Long> {

        private final java.util.concurrent.atomic.AtomicLong nextId =
                new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.ConcurrentHashMap<Long, Object> data =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public void loadNodePage(long pageId, java.util.Map<Bytes, Long> dst) {
            @SuppressWarnings("unchecked")
            java.util.Map<Bytes, Long> p = (java.util.Map<Bytes, Long>) data.get(pageId);
            if (p != null) {
                dst.putAll(p);
            }
        }

        @Override
        public void loadLeafPage(long pageId, java.util.Map<Bytes, Long> dst) {
            @SuppressWarnings("unchecked")
            java.util.Map<Bytes, Long> p = (java.util.Map<Bytes, Long>) data.get(pageId);
            if (p != null) {
                dst.putAll(p);
            }
        }

        @Override
        public long createNodePage(java.util.Map<Bytes, Long> page) {
            long id = nextId.incrementAndGet();
            data.put(id, new java.util.HashMap<>(page));
            return id;
        }

        @Override
        public long createLeafPage(java.util.Map<Bytes, Long> page) {
            long id = nextId.incrementAndGet();
            data.put(id, new java.util.HashMap<>(page));
            return id;
        }

        @Override
        public void overwriteNodePage(long pageId, java.util.Map<Bytes, Long> page) {
            data.put(pageId, new java.util.HashMap<>(page));
        }

        @Override
        public void overwriteLeafPage(long pageId, java.util.Map<Bytes, Long> page) {
            data.put(pageId, new java.util.HashMap<>(page));
        }
    }
}
