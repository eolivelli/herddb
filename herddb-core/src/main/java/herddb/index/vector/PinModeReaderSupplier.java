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

/**
 * Extension mixin for {@link ReaderSupplier} implementations that support
 * a pin-mode variant (issue #578).
 *
 * <p>A <em>pin-mode</em> supplier produces readers that insert every loaded
 * block into the frontier (eviction-resistant pinned) region of the
 * {@code SegmentBlockCache} rather than the ordinary evictable main cache.
 * This allows the warmup BFS to mark entry-frontier HNSW Layer-0 blocks as
 * high-value without touching the broader eviction policy.
 *
 * <p>Implementations that do not back a remote block cache (e.g. local-file
 * suppliers used in unit tests) need not implement this interface; callers
 * check for it with {@code instanceof} before invoking.
 *
 * @author enrico.olivelli
 */
public interface PinModeReaderSupplier {

    /**
     * Returns {@code true} when the underlying block cache has a configured
     * frontier (pinned) region with a positive byte budget. Callers skip the
     * pin BFS when this returns {@code false} — there is no benefit to calling
     * {@link #withPinMode()} if nowhere to put pinned blocks.
     */
    boolean hasFrontierCacheActive();

    /**
     * Returns a new {@link ReaderSupplier} whose readers use {@code pinMode=true}.
     * Every block loaded by a reader from the returned supplier is placed into
     * the frontier (pinned) region of the block cache instead of the main
     * evictable cache.
     *
     * <p>The returned supplier shares the same underlying block cache and
     * stats logger as this supplier; pinned blocks are immediately visible to
     * normal readers.
     */
    ReaderSupplier withPinMode();
}
