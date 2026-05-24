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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Decorator that wraps an inner {@link ObjectStorage} (e.g. S3) with a local disk cache.
 * <p>
 * Issue #475 reworked the on-disk layout to avoid the per-object
 * {@code open}/{@code close}/{@code create}/{@code delete} syscalls that the original
 * one-file-per-object layout incurred. The cache now writes payloads into two
 * {@link SlabCacheFileStore} tiers — a small-cell tier and a large-cell tier — kept
 * open for the JVM lifetime, with an in-memory index. Anything larger than the largest
 * tier's cell falls through to a per-file fallback (the original code path), which is
 * required for the rare case of metadata objects bigger than the configured large cell.
 * <p>
 * The cache is volatile: slab files are deleted on construction and on close, the cache
 * directory is wiped on construction, and there is no persistent on-disk index — the
 * Caffeine LRU starts empty on every boot. Caffeine's {@code maximumWeight} bounds the
 * <b>logical</b> bytes resident in the cache (aggregated across both slab tiers and the
 * fallback); the slab files themselves are pre-allocated to their per-tier byte budget.
 * <p>
 * Concurrent readers for the same missing key are deduplicated via {@code inFlightReads}
 * so only one {@code inner.read()} fires. Concurrent writers are deduplicated via
 * {@code inFlightWrites}.
 * <p>
 * Metrics (issue #336): Caffeine {@code recordStats()} is enabled so callers can query
 * hit/miss/eviction counts via {@link #diskLruStats()}. Byte-level hit/miss counters are
 * maintained via {@link AtomicLong} fields incremented in {@link #readRange}. Slab-tier
 * stats (issue #475) are exposed via {@link #getSmallSlab()} / {@link #getLargeSlab()}
 * and the fallback-tier stats via {@link #getFallbackFileCount()} /
 * {@link #getFallbackBytes()}. The maximum cache weight can be changed at runtime via
 * {@link #setMaxCacheBytes(long)}.
 *
 * @author enrico.olivelli
 */
public class CachingObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = Logger.getLogger(CachingObjectStorage.class.getName());

    /** Tier tag stored in {@link CacheEntry}. */
    static final byte TIER_SMALL_SLAB = 0;
    static final byte TIER_LARGE_SLAB = 1;
    static final byte TIER_FALLBACK = 2;

    /**
     * Configuration knob for slab tiers (issue #475). When {@link #slabEnabled} is
     * {@code false}, every admit goes through the per-file fallback path — the
     * original layout from before the slab refactor.
     */
    public static final class SlabConfig {
        public final boolean slabEnabled;
        public final int smallCellBytes;
        public final long smallTierBytes;
        public final int largeCellBytes;
        public final long largeTierBytes;

        public SlabConfig(boolean slabEnabled, int smallCellBytes, long smallTierBytes,
                          int largeCellBytes, long largeTierBytes) {
            this.slabEnabled = slabEnabled;
            this.smallCellBytes = smallCellBytes;
            this.smallTierBytes = smallTierBytes;
            this.largeCellBytes = largeCellBytes;
            this.largeTierBytes = largeTierBytes;
        }

        /** Default config: slab enabled, 64 KiB small cells (25%), 4 MiB large cells (75%). */
        public static SlabConfig defaultsFor(long totalBudgetBytes, int defaultLargeCellBytes) {
            int smallCell = 64 * 1024;
            long smallTier = Math.max(0L, totalBudgetBytes / 4);
            long largeTier = Math.max(0L, totalBudgetBytes - smallTier);
            int largeCell = defaultLargeCellBytes > 0 ? defaultLargeCellBytes : 4 * 1024 * 1024;
            return new SlabConfig(true, smallCell, smallTier, largeCell, largeTier);
        }

        /** Disabled config: every admit goes through the legacy per-file path. */
        public static SlabConfig disabled() {
            return new SlabConfig(false, 0, 0L, 0, 0L);
        }
    }

    /** In-LRU value: identifies which tier holds the payload. */
    static final class CacheEntry {
        final byte tier;
        final int payloadLength;

        CacheEntry(byte tier, int payloadLength) {
            this.tier = tier;
            this.payloadLength = payloadLength;
        }
    }

    private final ObjectStorage inner;
    private final Path cacheDir;
    private final ExecutorService executor;
    private final Cache<String, CacheEntry> diskLru;
    private final ConcurrentHashMap<String, CompletableFuture<ByteBuf>> inFlightReads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> inFlightWrites = new ConcurrentHashMap<>();

    private final boolean slabEnabled;
    private final SlabCacheFileStore smallSlab;
    private final SlabCacheFileStore largeSlab;
    /** Number of live entries currently held in the per-file fallback tier. */
    private final AtomicLong fallbackFileCount = new AtomicLong();
    /** Bytes held in the per-file fallback tier (sum of payload sizes). */
    private final AtomicLong fallbackBytes = new AtomicLong();

    /**
     * Bytes served from the on-disk cache in {@link #readRange} calls.
     * Incremented on a cache hit in the readRange hot path (issue #336).
     */
    private final AtomicLong hitBytes = new AtomicLong();

    /**
     * Bytes fetched from inner storage in {@link #readRange} calls (cache miss path).
     * Incremented when readRange falls through to the inner storage (issue #336).
     */
    private final AtomicLong missBytes = new AtomicLong();

    /**
     * Legacy constructor: defaults the slab layout from {@code maxCacheBytes} (small
     * tier 25%, large tier 75%, large cell 4 MiB). Kept for binary-compat with
     * existing tests and the {@code RemoteFileServer} reflection wiring.
     */
    public CachingObjectStorage(ObjectStorage inner, Path cacheDir, ExecutorService executor,
                                long maxCacheBytes) throws IOException {
        this(inner, cacheDir, executor, maxCacheBytes,
                SlabConfig.defaultsFor(maxCacheBytes, 4 * 1024 * 1024));
    }

    public CachingObjectStorage(ObjectStorage inner, Path cacheDir, ExecutorService executor,
                                long maxCacheBytes, SlabConfig slabConfig) throws IOException {
        this.inner = inner;
        this.cacheDir = cacheDir;
        this.executor = executor;
        clearCacheDir();
        Files.createDirectories(cacheDir);

        this.slabEnabled = slabConfig.slabEnabled;
        if (slabConfig.slabEnabled) {
            int smallCells = slabConfig.smallCellBytes > 0
                    ? (int) Math.min(Integer.MAX_VALUE, slabConfig.smallTierBytes / slabConfig.smallCellBytes)
                    : 0;
            int largeCells = slabConfig.largeCellBytes > 0
                    ? (int) Math.min(Integer.MAX_VALUE, slabConfig.largeTierBytes / slabConfig.largeCellBytes)
                    : 0;
            this.smallSlab = new SlabCacheFileStore(cacheDir.resolve("slab-small.dat"),
                    slabConfig.smallCellBytes > 0 ? slabConfig.smallCellBytes : 1, smallCells);
            this.largeSlab = new SlabCacheFileStore(cacheDir.resolve("slab-large.dat"),
                    slabConfig.largeCellBytes > 0 ? slabConfig.largeCellBytes : 1, largeCells);
        } else {
            this.smallSlab = null;
            this.largeSlab = null;
        }

        this.diskLru = Caffeine.newBuilder()
                .maximumWeight(maxCacheBytes)
                .weigher((String key, CacheEntry entry) -> {
                    if (entry == null) {
                        return 0;
                    }
                    return entry.payloadLength;
                })
                // recordStats() is required for hit/miss/eviction counters exposed via
                // diskLruStats(). The cost is negligible (a few AtomicLong increments
                // per cache operation).
                .recordStats()
                // evictionListener fires ONLY for size/time-based eviction — not for
                // explicit invalidate(...). This matches the original CachingObjectStorage
                // contract: explicit invalidations call onEviction synchronously from
                // invalidateAndDelete() so isInCache(...) and the slab/fallback gauges
                // are immediately consistent. Using removalListener here would fire for
                // explicit invalidations too and double-decrement the fallback counters.
                .evictionListener((RemovalListener<String, CacheEntry>) (key, entry, cause) -> {
                    // Best-effort cleanup of slab/fallback resources. For slab tiers,
                    // markEvicted is fast (atomic ops + map.remove + free-list push);
                    // for the fallback tier, we unlink the cache file. Concurrent
                    // readers that lose the race get a null/miss and fall through to
                    // inner.read() via the slab pin contract or via the
                    // NoSuchFileException catch in the per-file path.
                    if (key == null || entry == null) {
                        return;
                    }
                    onEviction(key, entry);
                })
                .build();
    }

    // -----------------------------------------------------------------------
    // Stats and dynamic resize (issue #336)
    // -----------------------------------------------------------------------

    /**
     * Returns a snapshot of Caffeine's stats for the disk-LRU index cache.
     * Provides cumulative hit, miss, eviction and eviction-weight counts since pod start.
     * Safe to call from gauge samplers (lock-free read of Caffeine's internal counters).
     */
    public CacheStats diskLruStats() {
        return diskLru.stats();
    }

    public long getHitBytes() {
        return hitBytes.get();
    }

    public long getMissBytes() {
        return missBytes.get();
    }

    public long estimatedEntries() {
        return diskLru.estimatedSize();
    }

    public long getMaxCacheBytes() {
        return diskLru.policy().eviction()
                .map(e -> e.getMaximum())
                .orElse(0L);
    }

    public long setMaxCacheBytes(long newMaxBytes) {
        if (newMaxBytes <= 0) {
            throw new IllegalArgumentException("newMaxBytes must be > 0, got: " + newMaxBytes);
        }
        long[] prev = {0L};
        diskLru.policy().eviction().ifPresent(e -> {
            prev[0] = e.getMaximum();
            e.setMaximum(newMaxBytes);
        });
        LOGGER.log(Level.INFO,
                "Disk cache resized: previousMax={0} bytes, newMax={1} bytes",
                new Object[]{prev[0], newMaxBytes});
        return prev[0];
    }

    // -----------------------------------------------------------------------
    // Slab-tier introspection (issue #475)
    // -----------------------------------------------------------------------

    /**
     * Returns the small-cell slab tier, or {@code null} when {@code cache.slab.enabled=false}.
     * Exposed primarily for Prometheus gauge wiring (see {@code RemoteFileServer}).
     */
    public SlabCacheFileStore getSmallSlab() {
        return smallSlab;
    }

    /**
     * Returns the large-cell slab tier, or {@code null} when {@code cache.slab.enabled=false}.
     */
    public SlabCacheFileStore getLargeSlab() {
        return largeSlab;
    }

    public long getFallbackFileCount() {
        return fallbackFileCount.get();
    }

    public long getFallbackBytes() {
        return fallbackBytes.get();
    }

    /** Returns true if {@code path} is currently resident in any tier. */
    public boolean isInCache(String path) {
        return diskLru.getIfPresent(path) != null;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private void clearCacheDir() throws IOException {
        if (Files.exists(cacheDir)) {
            try (Stream<Path> stream = Files.walk(cacheDir)) {
                stream.sorted((a, b) -> -a.compareTo(b))
                        .filter(p -> !p.equals(cacheDir))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Failed to delete cache file: " + p, e);
                            }
                        });
            }
        }
    }

    /**
     * Returns the fallback-tier cache-file path for {@code path}. Only meaningful
     * for entries admitted via the fallback (per-file) path; slab-tier entries do
     * not have an individual cache file. Used by {@link #onEviction} to unlink
     * fallback files and by tests that explicitly verify fallback semantics.
     */
    Path cacheFilePath(String path) {
        String safeName = path.replace('/', '_').replace('\\', '_');
        return cacheDir.resolve(safeName);
    }

    private void onEviction(String key, CacheEntry entry) {
        switch (entry.tier) {
            case TIER_SMALL_SLAB:
                if (smallSlab != null) {
                    smallSlab.markEvicted(key);
                }
                break;
            case TIER_LARGE_SLAB:
                if (largeSlab != null) {
                    largeSlab.markEvicted(key);
                }
                break;
            case TIER_FALLBACK:
                deleteFallbackFile(key, entry.payloadLength);
                break;
            default:
                LOGGER.log(Level.WARNING, "Unknown cache tier {0} for key {1}",
                        new Object[]{entry.tier, key});
        }
    }

    private void deleteFallbackFile(String path, int payloadLength) {
        try {
            if (Files.deleteIfExists(cacheFilePath(path))) {
                fallbackFileCount.decrementAndGet();
                fallbackBytes.addAndGet(-payloadLength);
            } else {
                // File already gone (e.g. clearCacheDir on construction racing with
                // a stale eviction notification, or a manual filesystem-level delete).
                // Clamp the counters at zero to keep the gauges non-negative under
                // any unexpected double-eviction.
                fallbackFileCount.updateAndGet(v -> v > 0 ? v - 1 : 0);
                fallbackBytes.updateAndGet(v -> v - payloadLength >= 0 ? v - payloadLength : 0);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete fallback cache file: " + path, e);
        }
    }

    /**
     * Asynchronously writes {@code bytes} to a fallback cache file using
     * {@link AsynchronousFileChannel}. Writes to a temp file then atomically renames.
     * On any failure we delete BOTH the partially-written temp file and the target
     * (the rename may have run before {@code completed} threw); leaving either
     * behind would be a per-cache-lifetime leak.
     */
    private CompletableFuture<Void> writeFallbackFileAsync(String path, byte[] bytes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Path target = cacheFilePath(path);
        Path tmp = cacheDir.resolve(target.getFileName() + ".tmp");

        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    tmp, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            channel.write(buffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesWritten, Void attachment) {
                    try {
                        channel.close();
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                        result.complete(null);
                    } catch (IOException e) {
                        bestEffortDelete(tmp);
                        bestEffortDelete(target);
                        result.completeExceptionally(e);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Channel close failure during error handling: safe to ignore.
                    }
                    bestEffortDelete(tmp);
                    bestEffortDelete(target);
                    result.completeExceptionally(exc);
                }
            });
        } catch (IOException t) {
            bestEffortDelete(tmp);
            bestEffortDelete(target);
            result.completeExceptionally(t);
        }

        return result;
    }

    private void bestEffortDelete(Path target) {
        try {
            Files.deleteIfExists(target);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete fallback file: " + target, e);
        }
    }

    /**
     * Asynchronously stores {@code content} in the cache and registers it in the disk LRU.
     * Returns a CompletableFuture that completes when the write is done.
     * Deduplicates concurrent writes to the same path via an in-flight map.
     * <p>
     * Tier selection (when {@code slabEnabled}):
     * <ol>
     *   <li>If {@code len &lt;= smallSlab.cellSize()} → try small slab first.</li>
     *   <li>Otherwise / on small-tier-full: if {@code len &lt;= largeSlab.cellSize()} → try large slab.</li>
     *   <li>Otherwise / on large-tier-full: fall through to the per-file fallback.</li>
     * </ol>
     * When {@code !slabEnabled}, every admit uses the per-file fallback.
     */
    private CompletableFuture<Void> admitToDisk(String path, byte[] content) {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> existing = inFlightWrites.putIfAbsent(path, pending);
        if (existing != null) {
            return existing;
        }
        admitChooseTier(path, content).whenComplete((v, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                pending.complete(null);
            } finally {
                inFlightWrites.remove(path, pending);
            }
        });
        return pending;
    }

    /**
     * ByteBuf overload of {@link #admitToDisk(String, byte[])}. The caller must hold
     * a refcount on {@code buf}; this method releases that refcount when the disk
     * write completes (success or failure). The buffer's reader/writer indices are
     * not modified.
     */
    private CompletableFuture<Void> admitToDisk(String path, ByteBuf buf) {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> existing = inFlightWrites.putIfAbsent(path, pending);
        if (existing != null) {
            buf.release();
            return existing;
        }
        admitChooseTier(path, buf).whenComplete((v, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                pending.complete(null);
            } finally {
                inFlightWrites.remove(path, pending);
            }
        });
        return pending;
    }

    private CompletableFuture<Void> admitChooseTier(String path, byte[] content) {
        if (!slabEnabled) {
            return admitToFallback(path, content);
        }
        int len = content.length;
        // Try small tier if it fits.
        if (smallSlab != null && len <= smallSlab.cellSize()) {
            ByteBuf bufForSmall = wrapHeap(content);
            return smallSlab.tryStore(path, bufForSmall).thenCompose(slot -> {
                if (slot != null) {
                    putEntry(path, new CacheEntry(TIER_SMALL_SLAB, len));
                    return CompletableFuture.completedFuture(null);
                }
                return tryLargeOrFallback(path, content);
            });
        }
        return tryLargeOrFallback(path, content);
    }

    private CompletableFuture<Void> tryLargeOrFallback(String path, byte[] content) {
        int len = content.length;
        if (largeSlab != null && len <= largeSlab.cellSize()) {
            ByteBuf bufForLarge = wrapHeap(content);
            return largeSlab.tryStore(path, bufForLarge).thenCompose(slot -> {
                if (slot != null) {
                    putEntry(path, new CacheEntry(TIER_LARGE_SLAB, len));
                    return CompletableFuture.completedFuture(null);
                }
                return admitToFallback(path, content);
            });
        }
        return admitToFallback(path, content);
    }

    /**
     * Wraps a heap byte[] into a {@link ByteBuf} suitable for {@link SlabCacheFileStore#tryStore}.
     * The slab path takes ownership of the refcount (releases when the async write
     * completes). Using {@link Unpooled#wrappedBuffer(byte[])} avoids a heap→direct copy
     * here; AsynchronousFileChannel internally stages a direct buffer for the actual write.
     */
    private static ByteBuf wrapHeap(byte[] content) {
        return Unpooled.wrappedBuffer(content);
    }

    /**
     * Inserts {@code entry} into the LRU under {@code path}, recycling any prior tier
     * resources from the previous entry under the same key (e.g. an admit that
     * promotes a fallback entry into a slab tier on rewrite).
     */
    private void putEntry(String path, CacheEntry entry) {
        CacheEntry prev = diskLru.asMap().put(path, entry);
        if (prev != null && prev != entry) {
            // Recycle prior tier resources for this path. For slab tiers, the slab's
            // own put() already overwrote the slot atomically — onEviction is a no-op.
            // For the fallback tier this unlinks the prior cache file.
            onEviction(path, prev);
        }
    }

    private CompletableFuture<Void> admitChooseTier(String path, ByteBuf buf) {
        int len = buf.readableBytes();
        if (!slabEnabled) {
            // Slab disabled: copy out and feed to the fallback path.
            byte[] heap = new byte[len];
            buf.getBytes(buf.readerIndex(), heap);
            buf.release();
            return admitToFallback(path, heap);
        }
        if (smallSlab != null && len <= smallSlab.cellSize()) {
            ByteBuf retainedForSmall = buf.retainedDuplicate();
            // whenComplete fires on BOTH success and exceptional completion so the
            // outer buf retain (held alongside the retainedDuplicate handed to the
            // slab) is released exactly once even when tryStore completes
            // exceptionally — thenCompose only runs on successful completion and
            // would otherwise leak the pooled direct buffer.
            return smallSlab.tryStore(path, retainedForSmall)
                    .whenComplete((slot, err) -> {
                        if (err != null) {
                            buf.release();
                        }
                    })
                    .thenCompose(slot -> {
                        if (slot != null) {
                            putEntry(path, new CacheEntry(TIER_SMALL_SLAB, len));
                            buf.release();
                            return CompletableFuture.completedFuture(null);
                        }
                        return tryLargeOrFallbackBuf(path, buf);
                    });
        }
        return tryLargeOrFallbackBuf(path, buf);
    }

    private CompletableFuture<Void> tryLargeOrFallbackBuf(String path, ByteBuf buf) {
        int len = buf.readableBytes();
        if (largeSlab != null && len <= largeSlab.cellSize()) {
            ByteBuf retainedForLarge = buf.retainedDuplicate();
            // Same exceptional-completion guard as admitChooseTier(ByteBuf): release
            // the outer buf retain whether tryStore resolves with a slot, returns
            // null (tier-full), or completes exceptionally.
            return largeSlab.tryStore(path, retainedForLarge)
                    .whenComplete((slot, err) -> {
                        if (err != null) {
                            buf.release();
                        }
                    })
                    .thenCompose(slot -> {
                        if (slot != null) {
                            putEntry(path, new CacheEntry(TIER_LARGE_SLAB, len));
                            buf.release();
                            return CompletableFuture.completedFuture(null);
                        }
                        return copyAndFallback(path, buf);
                    });
        }
        return copyAndFallback(path, buf);
    }

    private CompletableFuture<Void> copyAndFallback(String path, ByteBuf buf) {
        int len = buf.readableBytes();
        byte[] heap = new byte[len];
        buf.getBytes(buf.readerIndex(), heap);
        buf.release();
        return admitToFallback(path, heap);
    }

    private CompletableFuture<Void> admitToFallback(String path, byte[] content) {
        return writeFallbackFileAsync(path, content).thenApply(v -> {
            CacheEntry entry = new CacheEntry(TIER_FALLBACK, content.length);
            CacheEntry prev = diskLru.asMap().put(path, entry);
            if (prev == null || prev.tier != TIER_FALLBACK) {
                // Either there was no prior entry, or it was held in a slab tier.
                // The new file occupies one fallback slot and contributes its bytes.
                fallbackFileCount.incrementAndGet();
                fallbackBytes.addAndGet(content.length);
                if (prev != null) {
                    // Recycle the prior slab slot.
                    onEviction(path, prev);
                }
            } else {
                // Same-tier overwrite — only adjust bytes by the delta.
                fallbackBytes.addAndGet((long) content.length - prev.payloadLength);
            }
            return null;
        });
    }

    // -----------------------------------------------------------------------
    // ObjectStorage implementation
    // -----------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        // Single-object layout (issue #650): a full-file write replaces the
        // object at {@code path} in its entirety, so every block-cache entry
        // at {@code path#{blockIndex}} is now stale and must be evicted
        // before admitting the new full-file copy.
        invalidatePathAndBlocks(path);
        return inner.write(path, content).thenCompose(v -> admitToDisk(path, content));
    }

    /**
     * Single-object uploads use {@link #uploadFile} directly on the inner
     * storage (zero-copy CRT path on S3). We invalidate stale block-cache
     * entries for the path because the file content has just been replaced.
     * We do <em>not</em> admit the upload bytes into the cache here — the
     * file is on local disk in {@code source} until the caller releases it,
     * and prewarm via {@code prefetchRange} is the canonical way to populate
     * the file-server's cache for the new object.
     */
    @Override
    public CompletableFuture<Long> uploadFile(String path, java.nio.file.Path source,
                                              java.util.function.LongConsumer progress) {
        invalidatePathAndBlocks(path);
        return inner.uploadFile(path, source, progress);
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        return tryReadFromDiskAsync(path)
                .thenCompose(cached -> {
                    if (cached != null) {
                        return CompletableFuture.completedFuture(cached);
                    }
                    return loadAndCache(path);
                });
    }

    /**
     * Asynchronously attempts to read the full file from the on-disk cache (any
     * tier). Returns a future that completes with {@code null} on miss or
     * concurrent eviction. Caller MUST call {@link ReadResult#release()} when done
     * to return the buffer to the pool.
     */
    private CompletableFuture<ReadResult> tryReadFromDiskAsync(String path) {
        CacheEntry entry = diskLru.getIfPresent(path);
        if (entry == null) {
            return CompletableFuture.completedFuture(null);
        }
        switch (entry.tier) {
            case TIER_SMALL_SLAB:
                if (smallSlab == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return smallSlab.read(path).thenApply(buf -> buf == null ? null : ReadResult.found(buf));
            case TIER_LARGE_SLAB:
                if (largeSlab == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return largeSlab.read(path).thenApply(buf -> buf == null ? null : ReadResult.found(buf));
            case TIER_FALLBACK:
                return tryReadFromFallbackAsync(path);
            default:
                return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<ReadResult> tryReadFromFallbackAsync(String path) {
        CompletableFuture<ReadResult> result = new CompletableFuture<>();
        try {
            Path filePath = cacheFilePath(path);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);
            long fileSize = channel.size();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer((int) fileSize);
            ByteBuffer nioBuffer = byteBuf.nioBuffer(0, (int) fileSize);

            channel.read(nioBuffer, 0, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Channel close best-effort: data already in the buffer.
                    }
                    byteBuf.writerIndex(bytesRead);
                    result.complete(ReadResult.found(byteBuf));
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Channel close best-effort during error handling.
                    }
                    byteBuf.release();
                    // Treat as cache miss; fall through to inner storage.
                    result.complete(null);
                }
            });
        } catch (NoSuchFileException e) {
            // File was evicted after the LRU lookup; treat as cache miss.
            result.complete(null);
        } catch (IOException t) {
            LOGGER.log(Level.WARNING, "Failed to read fallback cache file for path: " + path, t);
            result.complete(null);
        }

        return result;
    }

    /**
     * Loads {@code path} from the inner storage, deduplicating concurrent loads of the
     * same key. On a successful load the bytes are written to the cache and registered
     * before being returned.
     */
    private CompletableFuture<ReadResult> loadAndCache(String path) {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        // Register the wrapDuplicate dependent BEFORE issuing inner.read. inner.read
        // may complete synchronously (e.g. test fakes returning a completed future),
        // and the disk-write completion runs on an I/O thread that races with this
        // method's return. Keeping the dependent attached up front guarantees it
        // fires inside pending.complete — while the alive-for-followers retain is
        // still held — instead of after the alive retain has already been released.
        CompletableFuture<ReadResult> resultFuture = pending.thenApply(CachingObjectStorage::wrapDuplicate);

        CompletableFuture<ByteBuf> existing = inFlightReads.putIfAbsent(path, pending);
        if (existing != null) {
            return existing.thenApply(CachingObjectStorage::wrapDuplicate);
        }
        inner.read(path).whenComplete((result, err) -> {
            if (err != null) {
                try {
                    pending.completeExceptionally(err);
                } finally {
                    inFlightReads.remove(path, pending);
                }
                return;
            }
            if (result.status() != ReadResult.Status.FOUND) {
                try {
                    pending.complete(null);
                } finally {
                    inFlightReads.remove(path, pending);
                }
                return;
            }
            ByteBuf buf = result.byteBuf();
            // Two extra retains:
            //   +1 for admitToDisk (released inside the tier write when finished).
            //   +1 to keep buf alive across pending.complete so all dependents
            //      (original caller + any in-flight followers) can retainedDuplicate.
            // The original `result`'s refcount is dropped right after.
            buf.retain(2);
            CompletableFuture<Void> diskWrite = admitToDisk(path, buf);
            try {
                diskWrite.whenComplete((v, e) -> {
                    try {
                        if (e != null) {
                            try {
                                pending.completeExceptionally(e);
                            } finally {
                                inFlightReads.remove(path, pending);
                            }
                            return;
                        }
                        try {
                            pending.complete(buf);
                        } finally {
                            inFlightReads.remove(path, pending);
                        }
                    } finally {
                        // Drop the alive-for-followers retain. Each successful follower
                        // already holds its own retainedDuplicate; failed completion has
                        // no followers to keep alive.
                        buf.release();
                    }
                });
            } finally {
                result.release();
            }
        });
        return resultFuture;
    }

    private static ReadResult wrapDuplicate(ByteBuf buf) {
        if (buf == null) {
            return ReadResult.notFound();
        }
        return ReadResult.found(buf.retainedDuplicate());
    }

    /**
     * Single-object layout (issue #650): every logical multipart file is one
     * S3 object at {@code path}. We cache slices of that object at
     * {@code blockSize}-aligned offsets, keyed by {@link #blockCacheKey(String, long)}
     * (a synthetic {@code path + "#" + blockIndex} string that lives in a
     * separate namespace from full-file cache entries used by {@link #read}).
     *
     * <p>Cache miss path: we ranged-read exactly one {@code blockSize}-byte
     * block from {@code inner} and cache the whole block, then slice the
     * portion the caller asked for. Subsequent reads inside the same block
     * are cache hits.
     */
    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        String blockKey = blockCacheKey(path, blockIndex);
        return tryReadSliceFromDiskAsync(blockKey, offsetInBlock, length)
                .thenCompose(cached -> {
                    if (cached != null) {
                        // Cache HIT: increment byte counter for Prometheus gauge.
                        hitBytes.addAndGet(length);
                        return CompletableFuture.completedFuture(cached);
                    }
                    // Cache MISS: increment byte counter and load one cache-block
                    // from the inner storage. The inner range read fetches at most
                    // {@code blockSize} bytes aligned to a block boundary; the
                    // returned slice may be SHORTER than blockSize for the tail
                    // block (last cache-block of the file).
                    missBytes.addAndGet(length);
                    long alignedOffset = blockIndex * (long) blockSize;
                    return loadAndCacheBlock(path, blockKey, alignedOffset, blockSize)
                            .thenApply(full -> sliceFromFull(full, offsetInBlock, length));
                });
    }

    /**
     * Cache-key namespace for block slices. Distinct from the full-file
     * namespace used by {@link #read} so the two never collide; the {@code #}
     * separator is chosen because object-storage keys must not contain
     * {@code #} (S3 / MinIO accept the byte but it is reserved as a URL
     * fragment delimiter and would never appear in a real backend key).
     */
    private static String blockCacheKey(String path, long blockIndex) {
        return path + "#" + blockIndex;
    }

    /**
     * Fetches the block-sized chunk at {@code alignedOffset} from the inner
     * storage, admits it into the cache under {@code blockKey}, and returns
     * a duplicated {@link ReadResult} for the caller to slice. Deduplicates
     * concurrent loads of the same block via {@link #inFlightReads}.
     */
    private CompletableFuture<ReadResult> loadAndCacheBlock(String path, String blockKey,
                                                            long alignedOffset, int blockSize) {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        CompletableFuture<ReadResult> resultFuture = pending.thenApply(CachingObjectStorage::wrapDuplicate);

        CompletableFuture<ByteBuf> existing = inFlightReads.putIfAbsent(blockKey, pending);
        if (existing != null) {
            return existing.thenApply(CachingObjectStorage::wrapDuplicate);
        }
        inner.readRange(path, alignedOffset, blockSize, blockSize).whenComplete((result, err) -> {
            if (err != null) {
                try {
                    pending.completeExceptionally(err);
                } finally {
                    inFlightReads.remove(blockKey, pending);
                }
                return;
            }
            if (result.status() != ReadResult.Status.FOUND) {
                try {
                    pending.complete(null);
                } finally {
                    inFlightReads.remove(blockKey, pending);
                }
                return;
            }
            ByteBuf buf = result.byteBuf();
            // Same refcount discipline as loadAndCache: +1 for admitToDisk, +1
            // to keep buf alive across followers' retainedDuplicate.
            buf.retain(2);
            CompletableFuture<Void> diskWrite = admitToDisk(blockKey, buf);
            try {
                diskWrite.whenComplete((v, e) -> {
                    try {
                        if (e != null) {
                            try {
                                pending.completeExceptionally(e);
                            } finally {
                                inFlightReads.remove(blockKey, pending);
                            }
                            return;
                        }
                        try {
                            pending.complete(buf);
                        } finally {
                            inFlightReads.remove(blockKey, pending);
                        }
                    } finally {
                        buf.release();
                    }
                });
            } finally {
                result.release();
            }
        });
        return resultFuture;
    }

    private CompletableFuture<ReadResult> tryReadSliceFromDiskAsync(String blockPath, int offsetInBlock, int length) {
        CacheEntry entry = diskLru.getIfPresent(blockPath);
        if (entry == null) {
            return CompletableFuture.completedFuture(null);
        }
        switch (entry.tier) {
            case TIER_SMALL_SLAB:
                if (smallSlab == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return smallSlab.readRange(blockPath, offsetInBlock, length)
                        .thenApply(buf -> buf == null ? null : ReadResult.found(buf));
            case TIER_LARGE_SLAB:
                if (largeSlab == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return largeSlab.readRange(blockPath, offsetInBlock, length)
                        .thenApply(buf -> buf == null ? null : ReadResult.found(buf));
            case TIER_FALLBACK:
                return tryReadSliceFromFallbackAsync(blockPath, offsetInBlock, length, entry.payloadLength);
            default:
                return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<ReadResult> tryReadSliceFromFallbackAsync(
            String blockPath, int offsetInBlock, int length, int blockLength) {
        if (offsetInBlock >= blockLength) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }
        int readable = Math.min(length, blockLength - offsetInBlock);
        CompletableFuture<ReadResult> result = new CompletableFuture<>();

        try {
            Path filePath = cacheFilePath(blockPath);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(readable);
            ByteBuffer nioBuffer = byteBuf.nioBuffer(0, readable);

            channel.read(nioBuffer, offsetInBlock, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Channel close best-effort: data already in the buffer.
                    }
                    byteBuf.writerIndex(bytesRead);
                    result.complete(ReadResult.found(byteBuf));
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // Channel close best-effort during error handling.
                    }
                    byteBuf.release();
                    result.complete(null);
                }
            });
        } catch (NoSuchFileException e) {
            result.complete(null);
        } catch (IOException t) {
            LOGGER.log(Level.WARNING,
                    "Failed to read slice from fallback cache file for path: " + blockPath, t);
            result.complete(null);
        }

        return result;
    }

    private static ReadResult sliceFromFull(ReadResult full, int offsetInBlock, int length) {
        if (full.status() == ReadResult.Status.NOT_FOUND) {
            full.release();
            return ReadResult.notFound();
        }
        try {
            ByteBuf blockBuf = full.byteBuf();
            int blockLength = blockBuf.readableBytes();
            if (offsetInBlock >= blockLength) {
                return ReadResult.notFound();
            }
            int to = Math.min(offsetInBlock + length, blockLength);
            int sliceLength = to - offsetInBlock;
            return ReadResult.found(blockBuf.retainedSlice(blockBuf.readerIndex() + offsetInBlock, sliceLength));
        } finally {
            full.release();
        }
    }

    /**
     * Explicit invalidation path: removes the entry from the LRU and synchronously
     * runs the per-tier cleanup. We do not rely on Caffeine's removal listener for
     * explicit invalidations because it may fire asynchronously on Caffeine's
     * default ForkJoinPool, which would leave the slab/fallback counters stale
     * relative to {@code isInCache(path)}.
     */
    private void invalidateAndDelete(String path) {
        CacheEntry removed = diskLru.asMap().remove(path);
        if (removed != null) {
            onEviction(path, removed);
        }
    }

    /**
     * Invalidates every cache entry whose key begins with {@code path} —
     * both the full-file entry at {@code path} itself (admitted via
     * {@link #read} / {@link #write}) and every block-cache entry at
     * {@code path#{blockIndex}} (admitted via {@link #readRange}).
     */
    private void invalidatePathAndBlocks(String path) {
        String blockPrefix = path + "#";
        List<String> toInvalidate = new ArrayList<>(diskLru.asMap().keySet());
        toInvalidate.stream()
                .filter(k -> k.equals(path) || k.startsWith(blockPrefix))
                .forEach(this::invalidateAndDelete);
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        invalidatePathAndBlocks(path);
        return inner.delete(path);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return inner.list(prefix);
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return inner.deleteByPrefix(prefix).thenApply(count -> {
            // Two key shapes to invalidate: (a) any full-file cache entry whose
            // path starts with prefix; (b) any block-cache entry whose composite
            // key {path}#{blockIndex} starts with prefix — these are caught by
            // the same startsWith() filter because the block-key prefix matches
            // the same path-prefix.
            List<String> keysToInvalidate = new ArrayList<>(diskLru.asMap().keySet());
            keysToInvalidate.stream()
                    .filter(k -> k.startsWith(prefix))
                    .forEach(this::invalidateAndDelete);
            return count;
        });
    }

    /**
     * Triggers pending Caffeine maintenance (evictions, removal listeners).
     * Package-private for use in tests.
     */
    void cleanUp() {
        diskLru.cleanUp();
    }

    @Override
    public void close() throws Exception {
        try {
            inner.close();
        } finally {
            IOException pending = null;
            if (smallSlab != null) {
                try {
                    smallSlab.close();
                } catch (IOException e) {
                    pending = e;
                }
            }
            if (largeSlab != null) {
                try {
                    largeSlab.close();
                } catch (IOException e) {
                    if (pending == null) {
                        pending = e;
                    }
                }
            }
            if (pending != null) {
                throw pending;
            }
        }
    }
}
