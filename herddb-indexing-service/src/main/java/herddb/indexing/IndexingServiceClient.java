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

import com.google.common.util.concurrent.ListenableFuture;
import herddb.index.vector.RemoteVectorIndexService;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.log.LogSequenceNumber;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.utils.Bytes;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC client for the IndexingService.
 * Manages connections to one or more IndexingService instances (primaries +
 * shadow replicas). Endpoints are grouped into pools keyed by effective
 * {@code instanceId} (a shadow shares its {@code shadowOf} primary's pool).
 * Search dispatches one RPC per pool, round-robin within the pool, with
 * fallback inside the pool on NOT_READY / retryable gRPC status.
 *
 * <p>Dynamic updates flow through
 * {@link #updateInstances(List)}. A volatile snapshot swap pattern ensures
 * lock-free reads in the hot path.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceClient implements RemoteVectorIndexService {

    private static final Logger LOGGER = Logger.getLogger(IndexingServiceClient.class.getName());

    private static final long DEFAULT_TIMEOUT_SECONDS = 30;

    private volatile ServerSnapshot snapshot;
    private final long timeoutSeconds;
    private final ClientInterceptor clientInterceptor;
    /**
     * Released once the client has seen at least one non-empty instance list,
     * either at construction or via {@link #updateInstances(List)}. Used by
     * {@link #awaitServersReady(long)} to let callers block on cold-cluster
     * ZK discovery before issuing the first RPC.
     */
    private final CountDownLatch serversReadyLatch = new CountDownLatch(1);

    /**
     * High-water mark of distinct channel addresses ever present in any
     * snapshot. Written only inside the constructor (single-threaded setup)
     * and inside {@link #updateInstances(List)} ({@code synchronized}).
     * A {@code volatile} read is sufficient for the unsynchronised
     * {@link #getMinProcessedLsn} hot-path.
     *
     * <p>Used to detect when an IS instance has been temporarily removed from
     * ZK (e.g. due to a GC pause or pod restart). When the current snapshot
     * has fewer channels than the peak, commit-log retention is pinned to
     * {@link LogSequenceNumber#START_OF_TIME} so that the server never drops
     * ledgers that the absent instance might still need. See issue #331.
     */
    private volatile int peakChannelCount = 0;

    /**
     * A single gRPC endpoint: an address + open channel, plus the role and
     * effective instanceId needed to group it into a pool.
     */
    static final class Endpoint {
        final String address;
        final ManagedChannel channel;
        final String role;
        final int effectiveInstanceId;

        Endpoint(String address, ManagedChannel channel, String role, int effectiveInstanceId) {
            this.address = address;
            this.channel = channel;
            this.role = role;
            this.effectiveInstanceId = effectiveInstanceId;
        }
    }

    /**
     * A shadow pool: the primary for a given effective instanceId plus its
     * shadow replicas. Search picks one endpoint per pool; on NOT_READY or
     * retryable failure the client falls back to another endpoint in the
     * same pool.
     */
    static final class Pool {
        final int effectiveInstanceId;
        final List<Endpoint> endpoints;
        final AtomicInteger rrCursor;

        Pool(int effectiveInstanceId, List<Endpoint> endpoints) {
            this.effectiveInstanceId = effectiveInstanceId;
            this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
            // Seed the round-robin cursor at a random offset so a cold-start
            // burst does not always hit the first endpoint.
            this.rrCursor = new AtomicInteger(
                    endpoints.isEmpty() ? 0 : ThreadLocalRandom.current().nextInt(endpoints.size()));
        }
    }

    private static class ServerSnapshot {
        /** Channels by address — iterated by {@link #getIndexStatus} and {@link #getMinProcessedLsn}. */
        final Map<String, ManagedChannel> channels;
        /** One pool per effective instanceId; iteration order is deterministic (sorted). */
        final List<Pool> pools;

        ServerSnapshot(Map<String, ManagedChannel> channels, List<Pool> pools) {
            this.channels = Collections.unmodifiableMap(new HashMap<>(channels));
            this.pools = Collections.unmodifiableList(new ArrayList<>(pools));
        }
    }

    /**
     * FAILED_PRECONDITION with this description prefix signals the
     * HerdDB-side client that the endpoint is a shadow replica that has not
     * yet completed its first reload (see {@code IndexingServiceImpl}).
     */
    private static final String SHADOW_NOT_READY_DESCRIPTION_PREFIX = "shadow_not_ready:";

    private static boolean isShadowNotReady(Throwable t) {
        while (t != null) {
            if (t instanceof StatusRuntimeException) {
                Status st = ((StatusRuntimeException) t).getStatus();
                if (st.getCode() == Status.Code.FAILED_PRECONDITION
                        && st.getDescription() != null
                        && st.getDescription().startsWith(SHADOW_NOT_READY_DESCRIPTION_PREFIX)) {
                    return true;
                }
                return false;
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean isRetryableTransportFailure(Throwable t) {
        while (t != null) {
            if (t instanceof StatusRuntimeException) {
                Status st = ((StatusRuntimeException) t).getStatus();
                return st.getCode() == Status.Code.UNAVAILABLE
                        || st.getCode() == Status.Code.DEADLINE_EXCEEDED;
            }
            t = t.getCause();
        }
        return false;
    }

    public IndexingServiceClient(List<IndexingServiceInstanceDescriptor> instances, long timeoutSeconds) {
        this(instances, timeoutSeconds, null);
    }

    public IndexingServiceClient(List<IndexingServiceInstanceDescriptor> instances,
                                 long timeoutSeconds, ClientInterceptor clientInterceptor) {
        this.timeoutSeconds = timeoutSeconds;
        this.clientInterceptor = clientInterceptor;
        ServerSnapshot initial = buildSnapshot(instances, Collections.emptyMap());
        this.snapshot = initial;
        this.peakChannelCount = initial.channels.size();
        if (!instances.isEmpty()) {
            this.serversReadyLatch.countDown();
        }
    }

    /**
     * Convenience factory for callers that only have a flat address list
     * (typically static configuration of a single-primary deployment): each
     * address becomes its own pool with a unique effective instanceId and
     * role=primary. For shadow-aware discovery use
     * {@link #IndexingServiceClient(List, long)} with descriptors.
     */
    public static IndexingServiceClient fromAddresses(List<String> addresses, long timeoutSeconds) {
        return fromAddresses(addresses, timeoutSeconds, null);
    }

    public static IndexingServiceClient fromAddresses(List<String> addresses, long timeoutSeconds,
                                                      ClientInterceptor interceptor) {
        List<IndexingServiceInstanceDescriptor> instances = new ArrayList<>(addresses.size());
        for (int i = 0; i < addresses.size(); i++) {
            String addr = addresses.get(i);
            instances.add(IndexingServiceInstanceDescriptor.primary(addr, addr, i));
        }
        return new IndexingServiceClient(instances, timeoutSeconds, interceptor);
    }

    private ManagedChannel buildChannel(String server) {
        ManagedChannelBuilder<?> b = ManagedChannelBuilder.forTarget(server)
                .usePlaintext()
                .disableRetry()  // eliminate UncommittedRetriableStreamsRegistry lock contention (issue #122)
                .keepAliveTime(300, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS);
        if (clientInterceptor != null) {
            b.intercept(clientInterceptor);
        }
        return b.build();
    }

    public boolean awaitServersReady(long timeoutMs) throws InterruptedException {
        return serversReadyLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Atomically replaces the pooled endpoint list. Groups instances by
     * effective instanceId (a shadow shares its {@code shadowOf} primary's
     * pool). Search fans out once per pool and falls back within a pool on
     * NOT_READY or a retryable gRPC status.
     */
    public synchronized void updateInstances(List<IndexingServiceInstanceDescriptor> newInstances) {
        if (newInstances == null || newInstances.isEmpty()) {
            LOGGER.log(Level.WARNING, "updateInstances called with empty list, keeping current servers");
            return;
        }

        ServerSnapshot current = this.snapshot;
        ServerSnapshot next = buildSnapshot(newInstances, current.channels);

        Set<String> removed = new LinkedHashSet<>(current.channels.keySet());
        removed.removeAll(next.channels.keySet());

        // Maintain high-water mark so getMinProcessedLsn can detect absent instances.
        if (next.channels.size() > peakChannelCount) {
            peakChannelCount = next.channels.size();
        }

        this.snapshot = next;
        serversReadyLatch.countDown();

        LOGGER.log(Level.INFO, "Updated indexing service instances: {0} pools, {1} endpoints",
                new Object[]{next.pools.size(), next.channels.size()});

        shutdownRemovedChannels(current, removed);
    }

    /**
     * Builds a {@link ServerSnapshot} from descriptors, reusing channels from
     * {@code existingChannels} where addresses overlap.
     */
    private ServerSnapshot buildSnapshot(List<IndexingServiceInstanceDescriptor> instances,
                                         Map<String, ManagedChannel> existingChannels) {
        Map<String, ManagedChannel> newChannels = new HashMap<>();
        for (IndexingServiceInstanceDescriptor d : instances) {
            if (!newChannels.containsKey(d.getAddress())) {
                ManagedChannel existing = existingChannels.get(d.getAddress());
                newChannels.put(d.getAddress(),
                        existing != null ? existing : buildChannel(d.getAddress()));
            }
        }
        TreeMap<Integer, List<Endpoint>> byInstance = new TreeMap<>();
        for (IndexingServiceInstanceDescriptor d : instances) {
            int effective = d.effectiveInstanceId();
            ManagedChannel ch = newChannels.get(d.getAddress());
            byInstance.computeIfAbsent(effective, k -> new ArrayList<>())
                    .add(new Endpoint(d.getAddress(), ch, d.getRole(), effective));
        }
        List<Pool> pools = new ArrayList<>(byInstance.size());
        for (Map.Entry<Integer, List<Endpoint>> e : byInstance.entrySet()) {
            pools.add(new Pool(e.getKey(), e.getValue()));
        }
        return new ServerSnapshot(newChannels, pools);
    }

    private void shutdownRemovedChannels(ServerSnapshot current, Set<String> removed) {
        if (removed.isEmpty()) {
            return;
        }
        List<ManagedChannel> toShutdown = new ArrayList<>();
        for (String server : removed) {
            ManagedChannel ch = current.channels.get(server);
            if (ch != null) {
                toShutdown.add(ch);
            }
        }
        Thread shutdownThread = new Thread(() -> {
            for (ManagedChannel ch : toShutdown) {
                try {
                    ch.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ch.shutdownNow();
                }
            }
        }, "indexing-channel-shutdown");
        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    /**
     * Searches the IndexingService cluster and returns the top-{@code limit}
     * entries by score.
     *
     * <p>With multiple instances, the search RPC is fanned out to <b>every</b>
     * configured instance in parallel (each instance owns a subset of the
     * graph shards, so partial results would be incorrect). Results are
     * merged into a bounded min-heap of size {@code limit} as each future
     * completes. If any RPC fails, the remaining in-flight RPCs are
     * cancelled and the whole call fails fast.
     *
     * <p>With a single instance, a blocking fast-path skips the future
     * machinery.
     */
    public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                  float[] vector, int limit) {
        ServerSnapshot s = this.snapshot;

        if (s.pools.isEmpty()) {
            throw new RuntimeException("No indexing service instances available");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("search limit must be positive, got " + limit);
        }

        boolean multiPool = s.pools.size() > 1;
        boolean returnScore = multiPool; // always request scores when merging multiple pools

        LOGGER.log(Level.FINE,
                "client search: tablespace={0}, table={1}, index={2}, limit={3}, vectorDim={4}, pools={5}, endpoints={6}",
                new Object[]{tablespace, table, index, limit, vector.length, s.pools.size(), s.channels.size()});
        long start = System.nanoTime();

        SearchRequest.Builder requestBuilder = SearchRequest.newBuilder()
                .setTablespace(tablespace)
                .setTable(table)
                .setIndex(index)
                .setLimit(limit)
                .setReturnScore(returnScore);
        for (float v : vector) {
            requestBuilder.addVector(v);
        }
        SearchRequest request = requestBuilder.build();

        if (!multiPool && s.pools.get(0).endpoints.size() == 1) {
            // Single endpoint in a single pool: blocking fast-path.
            Endpoint only = s.pools.get(0).endpoints.get(0);
            IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                    IndexingServiceGrpc.newBlockingStub(only.channel)
                            .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
            SearchResponse response = stub.search(request);
            List<Map.Entry<Bytes, Float>> results = toEntryList(response);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.FINE, "client search completed (single endpoint): index={0}, {1} results in {2} ms",
                    new Object[]{index, results.size(), elapsedMs});
            return results;
        }

        // Per-pool fan-out. For each pool we dispatch ONE RPC (round-robin
        // starting point), await it, and on NOT_READY / retryable failure we
        // retry against a different endpoint in the same pool. If every
        // endpoint in a pool fails, the whole search fails.
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        List<Map.Entry<String, ListenableFuture<SearchResponse>>> inflight = new ArrayList<>(s.pools.size());
        // Endpoints already dispatched per pool so we don't retry the same one.
        List<Set<String>> attempted = new ArrayList<>(s.pools.size());
        for (int i = 0; i < s.pools.size(); i++) {
            Pool p = s.pools.get(i);
            Endpoint ep = pickEndpoint(p, Collections.emptySet());
            attempted.add(new HashSet<>(Collections.singletonList(ep.address)));
            inflight.add(dispatchSearch(ep, request, deadlineNanos));
        }

        int initialCapacity = Math.max(1, Math.min(limit, 1024));
        PriorityQueue<Map.Entry<Bytes, Float>> topK =
                new PriorityQueue<>(initialCapacity, Comparator.comparing(Map.Entry::getValue));

        for (int i = 0; i < inflight.size(); i++) {
            Pool pool = s.pools.get(i);
            SearchResponse response;
            while (true) {
                Map.Entry<String, ListenableFuture<SearchResponse>> f = inflight.get(i);
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    cancelAll(inflight);
                    throw new RuntimeException("Search timed out waiting for indexing-service pool "
                            + pool.effectiveInstanceId + " after " + timeoutSeconds + "s");
                }
                try {
                    response = f.getValue().get(remaining, TimeUnit.NANOSECONDS);
                    break;
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (isShadowNotReady(cause) || isRetryableTransportFailure(cause)) {
                        Endpoint fallback = pickEndpoint(pool, attempted.get(i));
                        if (fallback == null) {
                            cancelAll(inflight);
                            throw new RuntimeException("Search failed on indexing-service pool "
                                    + pool.effectiveInstanceId + " (all "
                                    + pool.endpoints.size() + " endpoints exhausted, last from "
                                    + f.getKey() + ")", cause);
                        }
                        LOGGER.log(Level.FINE,
                                "Search on pool {0} fell back from {1} to {2} after {3}",
                                new Object[]{pool.effectiveInstanceId, f.getKey(), fallback.address,
                                        cause == null ? "null" : cause.getMessage()});
                        attempted.get(i).add(fallback.address);
                        inflight.set(i, dispatchSearch(fallback, request, deadlineNanos));
                        continue;
                    }
                    cancelAll(inflight);
                    throw new RuntimeException("Search failed on indexing-service pool "
                            + pool.effectiveInstanceId + " (" + f.getKey() + ") — " + cause, cause);
                } catch (TimeoutException te) {
                    cancelAll(inflight);
                    throw new RuntimeException("Search timed out waiting for indexing-service pool "
                            + pool.effectiveInstanceId + " after " + timeoutSeconds + "s", te);
                } catch (InterruptedException ie) {
                    cancelAll(inflight);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Search interrupted while waiting for indexing-service pools", ie);
                }
            }
            for (Map.Entry<Bytes, Float> e : toEntryList(response)) {
                if (topK.size() < limit) {
                    topK.offer(e);
                } else if (e.getValue() > topK.peek().getValue()) {
                    topK.poll();
                    topK.offer(e);
                }
            }
        }

        List<Map.Entry<Bytes, Float>> out = new ArrayList<>(topK);
        out.sort(Comparator.<Map.Entry<Bytes, Float>, Float>comparing(Map.Entry::getValue).reversed());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        LOGGER.log(Level.FINE, "client search completed (multi-pool fan-out): index={0}, {1} results in {2} ms",
                new Object[]{index, out.size(), elapsedMs});
        return out;
    }

    /** Round-robin endpoint picker, skipping addresses already attempted. */
    private static Endpoint pickEndpoint(Pool pool, Set<String> skipAddresses) {
        int n = pool.endpoints.size();
        if (n == 0) {
            return null;
        }
        for (int i = 0; i < n; i++) {
            int idx = Math.floorMod(pool.rrCursor.getAndIncrement(), n);
            Endpoint ep = pool.endpoints.get(idx);
            if (!skipAddresses.contains(ep.address)) {
                return ep;
            }
        }
        return null;
    }

    private Map.Entry<String, ListenableFuture<SearchResponse>> dispatchSearch(
            Endpoint ep, SearchRequest request, long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            remaining = 1;
        }
        IndexingServiceGrpc.IndexingServiceFutureStub stub =
                IndexingServiceGrpc.newFutureStub(ep.channel)
                        .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS);
        return new AbstractMap.SimpleImmutableEntry<>(ep.address, stub.search(request));
    }

    private static void cancelAll(List<Map.Entry<String, ListenableFuture<SearchResponse>>> futures) {
        for (Map.Entry<String, ListenableFuture<SearchResponse>> f : futures) {
            if (!f.getValue().isDone()) {
                f.getValue().cancel(true);
            }
        }
    }

    private static String failingAddress(List<Map.Entry<String, ListenableFuture<SearchResponse>>> futures) {
        for (Map.Entry<String, ListenableFuture<SearchResponse>> f : futures) {
            ListenableFuture<SearchResponse> lf = f.getValue();
            if (lf.isDone() && !lf.isCancelled()) {
                try {
                    lf.get(0, TimeUnit.MILLISECONDS);
                } catch (ExecutionException ex) {
                    return f.getKey();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return f.getKey();
                } catch (TimeoutException ex) {
                    // still running — not the culprit
                }
            }
        }
        return "<unknown>";
    }

    @Override
    public RemoteVectorIndexService.IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
        ServerSnapshot s = this.snapshot;

        if (s.channels.isEmpty()) {
            throw new RuntimeException("No indexing service instances available");
        }

        // Query the first available instance that is ready (skip NOT_READY
        // shadows transparently).
        for (Map.Entry<String, ManagedChannel> entry : s.channels.entrySet()) {
            try {
                IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                        IndexingServiceGrpc.newBlockingStub(entry.getValue())
                                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
                GetIndexStatusResponse resp = stub.getIndexStatus(GetIndexStatusRequest.newBuilder()
                        .setTablespace(tablespace)
                        .setTable(table)
                        .setIndex(index)
                        .build());
                return new RemoteVectorIndexService.IndexStatusInfo(
                        resp.getVectorCount(), resp.getSegmentCount(),
                        resp.getTailerLsnLedger(), resp.getTailerLsnOffset(),
                        resp.getTailerLsnTimestamp(),
                        resp.getDurableLsnLedger(), resp.getDurableLsnOffset(),
                        resp.getDurableLsnTimestamp(),
                        resp.getStatus(),
                        resp.getLoadingSegmentsDone(), resp.getLoadingSegmentsTotal());
            } catch (Exception e) {
                if (isShadowNotReady(e)) {
                    LOGGER.log(Level.FINE, "GetIndexStatus: skipping NOT_READY shadow {0}", entry.getKey());
                } else {
                    LOGGER.log(Level.WARNING, "GetIndexStatus failed on instance " + entry.getKey(), e);
                }
            }
        }
        throw new RuntimeException("All IndexingService instances failed for GetIndexStatus");
    }

    private static List<Map.Entry<Bytes, Float>> toEntryList(SearchResponse response) {
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>(response.getResultsCount());
        for (SearchResult result : response.getResultsList()) {
            byte[] pkBytes = result.getPrimaryKey().toByteArray();
            results.add(new AbstractMap.SimpleImmutableEntry<>(
                    Bytes.from_array(pkBytes), result.getScore()));
        }
        return results;
    }

    private static final long CATCHUP_POLL_INTERVAL_MS = 5000;

    @Override
    public boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber, long timeoutMs) throws InterruptedException {
        ServerSnapshot s = this.snapshot;
        if (s.channels.isEmpty()) {
            LOGGER.log(Level.WARNING, "waitForCatchUp: no indexing service instances available");
            return false;
        }
        for (Map.Entry<String, ManagedChannel> serverEntry : s.channels.entrySet()) {
            String server = serverEntry.getKey();
            ManagedChannel channel = serverEntry.getValue();
            if (!waitForInstanceCatchUp(server, channel, tablespace, sequenceNumber, timeoutMs)) {
                return false;
            }
        }
        return true;
    }

    private boolean waitForInstanceCatchUp(String server, ManagedChannel channel,
                                         String tablespace,
                                         LogSequenceNumber target,
                                         long timeoutMs) throws InterruptedException {
        long now = System.currentTimeMillis();
        long deadline = (timeoutMs > Long.MAX_VALUE - now) ? Long.MAX_VALUE : now + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                        IndexingServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
                GetIndexStatusResponse resp = stub.getIndexStatus(GetIndexStatusRequest.newBuilder()
                        .setTablespace(tablespace)
                        .setTable("")
                        .setIndex("")
                        .build());
                // WAITFORINDEXES is a queryability gate: the caller wants
                // to know that the IS has applied every entry up to {@code
                // target} into its in-memory state, so a search issued
                // immediately after returns those rows. Compare against the
                // tailer LSN (in-memory progress), NOT the durable LSN —
                // requiring durability here would force a checkpoint to
                // succeed before WAITFORINDEXES returns, which has nothing
                // to do with what the SQL caller is asking. Recovery / log
                // retention is a SEPARATE concern, handled by
                // {@link #getMinProcessedLsn} which DOES read the durable
                // LSN (issue #364).
                LogSequenceNumber tailerLsn = new LogSequenceNumber(
                        resp.getTailerLsnLedger(), resp.getTailerLsnOffset());
                if (tailerLsn.after(target) || tailerLsn.equals(target)) {
                    LOGGER.log(Level.INFO, "Instance {0} caught up for tablespace {1} to {2} (tailer={3})",
                            new Object[]{server, tablespace, target, tailerLsn});
                    return true;
                }
                LOGGER.log(Level.INFO,
                        "Instance {0} tailer={1} (durable={2}/{3}) for tablespace {4}, "
                                + "waiting for {5} (status: {6})",
                        new Object[]{server, tailerLsn,
                                resp.getDurableLsnLedger(), resp.getDurableLsnOffset(),
                                tablespace, target, resp.getStatus()});
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Instance {0} unreachable for tablespace {1}, retrying: {2}",
                        new Object[]{server, tablespace, e.getMessage()});
            }
            Thread.sleep(CATCHUP_POLL_INTERVAL_MS);
        }
        LOGGER.log(Level.WARNING, "Instance {0} did not catch up for tablespace {1} to {2} within timeout ({3} ms)",
                new Object[]{server, tablespace, target, timeoutMs});
        return false;
    }

    @Override
    public Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
        ServerSnapshot s = this.snapshot;
        if (s.channels.isEmpty()) {
            return Optional.empty();
        }

        // If fewer IS instances are reachable than the peak ever seen, at least
        // one instance has temporarily left ZK (e.g. GC pause, pod restart).
        // Pin retention to START_OF_TIME so the server never drops commit-log
        // ledgers that the absent instance might still need when it comes back.
        // Issue #331: without this guard, dropOldLedgers used only the
        // connected instance's position as the floor, dropping ledgers that
        // the offline instance required — making it permanently unrecoverable.
        int peak = peakChannelCount;
        if (s.channels.size() < peak) {
            LOGGER.log(Level.WARNING,
                    "getMinProcessedLsn: only {0}/{1} IS instance(s) reachable for tablespace {2} "
                    + "(peak was {1}); pinning commit-log retention at START_OF_TIME to protect "
                    + "the absent instance(s)'' commit-log position",
                    new Object[]{s.channels.size(), peak, tablespace});
            return Optional.of(LogSequenceNumber.START_OF_TIME);
        }

        LogSequenceNumber min = null;
        for (Map.Entry<String, ManagedChannel> serverEntry : s.channels.entrySet()) {
            String server = serverEntry.getKey();
            ManagedChannel channel = serverEntry.getValue();
            try {
                IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                        IndexingServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
                GetIndexStatusResponse resp = stub.getIndexStatus(GetIndexStatusRequest.newBuilder()
                        .setTablespace(tablespace)
                        .setTable("")
                        .setIndex("")
                        .build());
                // Read the DURABLE LSN, not the in-memory tailer position
                // (issue #364). The server's commit-log retention floor MUST
                // be anchored at the LSN from which the IS could resume on a
                // restart — otherwise it can drop ledgers the IS would need
                // to replay, just because its tailer had momentarily advanced
                // past them in memory without a successful checkpoint.
                LogSequenceNumber instanceLsn = readDurableLsnOrStartOfTime(resp);
                if (LogSequenceNumber.START_OF_TIME.equals(instanceLsn)) {
                    // Sentinel: instance has not yet published a durable
                    // watermark. Pin retention immediately — same protection
                    // path as for an unreachable instance.
                    LOGGER.log(Level.WARNING,
                            "getMinProcessedLsn: instance {0} has no durable watermark yet "
                                    + "for tablespace {1} (tailer={2}/{3}); "
                                    + "pinning commit-log retention at START_OF_TIME",
                            new Object[]{server, tablespace,
                                    resp.getTailerLsnLedger(), resp.getTailerLsnOffset()});
                    return Optional.of(LogSequenceNumber.START_OF_TIME);
                }
                if (min == null || min.after(instanceLsn)) {
                    min = instanceLsn;
                }
            } catch (RuntimeException e) {
                // Plugin boundary: any gRPC failure means we can't be sure the tailer
                // has made progress, so we force maximum retention.
                LOGGER.log(Level.WARNING,
                        "getMinProcessedLsn: instance {0} unreachable for tablespace {1}: {2}",
                        new Object[]{server, tablespace, e.getMessage()});
                return Optional.of(LogSequenceNumber.START_OF_TIME);
            }
        }
        return Optional.ofNullable(min);
    }

    /**
     * Decodes the durable recovery LSN advertised by an IS instance via
     * {@link GetIndexStatusResponse#getDurableLsnLedger()} /
     * {@link GetIndexStatusResponse#getDurableLsnOffset()}. Treats the
     * proto-default {@code (0, 0)} pair as
     * {@link LogSequenceNumber#START_OF_TIME}: this happens either because
     * the instance has not yet completed a checkpoint (and therefore has no
     * durable recovery point) or because the field is unset on the wire.
     * In both cases the safe interpretation is "do not advance retention",
     * so callers see the START_OF_TIME sentinel and pin accordingly
     * (issue #364).
     */
    private static LogSequenceNumber readDurableLsnOrStartOfTime(GetIndexStatusResponse resp) {
        long ledger = resp.getDurableLsnLedger();
        long offset = resp.getDurableLsnOffset();
        if (ledger <= 0 && offset <= 0) {
            return LogSequenceNumber.START_OF_TIME;
        }
        return new LogSequenceNumber(ledger, offset);
    }

    public List<String> getServers() {
        return new ArrayList<>(snapshot.channels.keySet());
    }

    @Override
    public void close() {
        ServerSnapshot s = this.snapshot;
        for (ManagedChannel channel : s.channels.values()) {
            try {
                channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
}
