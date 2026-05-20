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

import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;

/**
 * Extension mixin for {@link ReaderSupplier} implementations that can populate
 * the local block cache in bulk via a small number of large I/O reads
 * (issue #619).
 *
 * <p>The default warmup path executes a BFS over the HNSW Layer-0 graph and
 * lets the per-thread {@link io.github.jbellis.jvector.disk.RandomAccessReader}
 * pull one cache-block at a time. With a 32 MiB warmup budget and 16 KiB cache
 * blocks that is up to ~2000 individual {@code readFileRange} round-trips per
 * segment — fast on a local file system, but devastatingly slow when the
 * indexing service is backed by the remote file server / S3 / GCS.
 *
 * <p>Suppliers that implement this mixin can short-circuit that BFS by reading
 * a contiguous prefix of the segment file in a few large multipart-chunk-sized
 * RPCs and bulk-inserting every covered block into the shared
 * {@code SegmentBlockCache}. The subsequent BFS pass then hits the cache for
 * every read — no wire I/O — preserving the per-segment "warmed" accounting
 * and the pin BFS pass for the frontier region while removing the dominant
 * latency.
 *
 * <p>Implementations that do not back a remote, network-bound storage (e.g.
 * local-file suppliers used in unit tests, or pass-through in-memory readers)
 * need not implement this interface; callers check for it with
 * {@code instanceof} and fall back to the default BFS path when it is absent.
 *
 * <p><b>Layout assumption.</b> Callers using this interface for HNSW warmup
 * (notably {@code PersistentVectorStore.bulkPrefetchSegmentIntoCache}) assume
 * that the bytes the warmup BFS would have visited — the HNSW entry-frontier
 * on Layer 0 — live in the file's leading prefix. This holds for the current
 * jvector {@code OnDiskGraphIndex} layout where Layer 0 data dominates the
 * file from a small header onwards. If a future jvector layout change moves
 * the entry-frontier later in the file, the prefetch still populates the
 * cache correctly but covers the wrong bytes; the subsequent BFS then degrades
 * to pre-#619 per-node wire reads — no correctness impact, only a missed
 * optimisation. Implementations therefore do not need to know about the HNSW
 * layout; they just deliver the bytes requested.
 *
 * @author enrico.olivelli
 */
public interface BulkPrefetchReaderSupplier {

    /**
     * Prefetches up to {@code maxBytes} bytes starting at {@code startOffset}
     * from the underlying file into the local block cache.
     *
     * <p>The implementation issues a small number of large reads (one per
     * underlying multipart chunk, executed in parallel) and bulk-inserts every
     * covered cache-block into the {@code SegmentBlockCache}. After this
     * method returns successfully every block in
     * {@code [startOffset, startOffset + bytesCovered)} is resident in the
     * main cache region; subsequent reads via a normal
     * {@code RandomAccessReader} on the same {@code (path, blockSize)} hit the
     * cache without wire I/O.
     *
     * <p>The method is best-effort and idempotent: a block that is already
     * cached is left untouched (existing entries are never evicted by this
     * call), and a partial read at end-of-file is allowed.
     *
     * @param startOffset starting byte offset within the file; must be
     *     non-negative and aligned to the underlying multipart chunk size
     * @param maxBytes    maximum number of bytes to read; the actual amount
     *     read is clamped to {@code fileSize - startOffset}
     * @return number of bytes actually transferred from storage into the
     *     cache (may be less than {@code maxBytes} at end-of-file)
     * @throws IOException if the storage backend fails; the caller must treat
     *     this as best-effort and fall back to the per-block BFS path
     */
    long bulkPrefetchIntoCache(long startOffset, long maxBytes) throws IOException;
}
