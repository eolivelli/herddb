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

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkIndexDataStorage;
import herddb.index.blink.BytesLongSizeEvaluator;
import herddb.utils.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only factory for {@link BLink}-backed structures used by the vector
 * index — replaces the dependency on {@link PersistentVectorStore}'s factory
 * methods so tests can exercise {@link CompactionAuthorityMap} and
 * {@link PagedPkSet} without spinning up a full store.
 *
 * <p>Each helper returns a wrapper backed by an in-memory
 * {@link BLinkIndexDataStorage}; the cleanup callback is a no-op since
 * there is no underlying disk storage to drop.
 */
final class TestBLinks {

    /** Default page size for test BLinks. Small enough to exercise paging. */
    static final long DEFAULT_PAGE_SIZE = 4 * 1024L;

    /** Default page replacement policy capacity (pages held resident). */
    static final int DEFAULT_POLICY_CAPACITY = 4;

    private TestBLinks() {
    }

    /**
     * Returns a new {@link CompactionAuthorityMap} backed by a small
     * in-memory BLink. The cleanup hook is a no-op.
     */
    static CompactionAuthorityMap inMemoryCompactionAuthorityMap() {
        BLink<Bytes, Long> blink = newInMemoryBytesLongBLink(
                DEFAULT_PAGE_SIZE, DEFAULT_POLICY_CAPACITY);
        return new CompactionAuthorityMap(blink, () -> { });
    }

    /**
     * Returns a new {@link CompactionAuthorityMap} backed by an in-memory
     * BLink configured with the supplied page size and resident-page
     * capacity. Useful for spill-coverage tests.
     */
    static CompactionAuthorityMap inMemoryCompactionAuthorityMap(long pageSize, int policyCapacity) {
        BLink<Bytes, Long> blink = newInMemoryBytesLongBLink(pageSize, policyCapacity);
        return new CompactionAuthorityMap(blink, () -> { });
    }

    /**
     * Returns a new {@link PagedPkSet} backed by a small in-memory BLink.
     * The cleanup hook is a no-op.
     */
    static PagedPkSet inMemoryPagedPkSet() {
        BLink<Bytes, Long> blink = newInMemoryBytesLongBLink(
                DEFAULT_PAGE_SIZE, DEFAULT_POLICY_CAPACITY);
        return new PagedPkSet(blink, () -> { });
    }

    /**
     * Returns an in-memory {@code BLink<Bytes, Long>} suitable for use as a
     * test stand-in for {@link VectorSegment#onDiskPkToNode}.
     */
    static BLink<Bytes, Long> inMemoryPkToNode() {
        return newInMemoryBytesLongBLink(DEFAULT_PAGE_SIZE, DEFAULT_POLICY_CAPACITY);
    }

    private static BLink<Bytes, Long> newInMemoryBytesLongBLink(long pageSize, int policyCapacity) {
        PageReplacementPolicy policy = new RandomPageReplacementPolicy(policyCapacity);
        return new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                policy, new InMemoryBytesLongStorage());
    }

    /**
     * In-memory {@link BLinkIndexDataStorage} for {@code (Bytes, Long)} BLinks.
     * Pages are stored in a {@link ConcurrentHashMap}; the same instance is
     * never shared across BLinks (each test factory call creates a fresh one).
     */
    private static final class InMemoryBytesLongStorage implements BLinkIndexDataStorage<Bytes, Long> {
        private final AtomicLong newPageId = new AtomicLong();
        private final ConcurrentHashMap<Long, Object> datas = new ConcurrentHashMap<>();

        @Override
        public void loadNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            @SuppressWarnings("unchecked")
            Map<Bytes, Long> res = (Map<Bytes, Long>) datas.get(pageId);
            if (res != null) {
                data.putAll(res);
            }
        }

        @Override
        public void loadLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            @SuppressWarnings("unchecked")
            Map<Bytes, Long> res = (Map<Bytes, Long>) datas.get(pageId);
            if (res != null) {
                data.putAll(res);
            }
        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public long createLeafPage(Map<Bytes, Long> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }
    }
}
