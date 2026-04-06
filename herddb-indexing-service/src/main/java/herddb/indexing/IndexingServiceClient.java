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

import herddb.index.vector.RemoteVectorIndexService;
import herddb.indexing.proto.GetIndexStatusRequest;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.IndexingServiceGrpc;
import herddb.indexing.proto.SearchRequest;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.log.LogSequenceNumber;
import herddb.server.DynamicServiceClient;
import herddb.utils.Bytes;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC client for the IndexingService.
 * Manages connections to one or more IndexingService instances (full replicas).
 * <p>
 * Search fans out to all instances and merges results by score.
 * If only one instance is configured, results are returned as-is (already sorted by similarity).
 * <p>
 * Supports dynamic server list updates via {@link #updateServers(List)}.
 * A volatile snapshot swap pattern ensures lock-free reads in the hot path.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceClient implements RemoteVectorIndexService, DynamicServiceClient {

    private static final Logger LOGGER = Logger.getLogger(IndexingServiceClient.class.getName());

    private static final long DEFAULT_TIMEOUT_SECONDS = 30;

    private volatile ServerSnapshot snapshot;
    private final long timeoutSeconds;
    private final ClientInterceptor clientInterceptor;

    private static class ServerSnapshot {
        final List<String> servers;
        final Map<String, ManagedChannel> channels;

        ServerSnapshot(List<String> servers, Map<String, ManagedChannel> channels) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
            this.channels = Collections.unmodifiableMap(new HashMap<>(channels));
        }
    }

    public IndexingServiceClient(List<String> servers, long timeoutSeconds) {
        this(servers, timeoutSeconds, null);
    }

    public IndexingServiceClient(List<String> servers, long timeoutSeconds, ClientInterceptor clientInterceptor) {
        this.timeoutSeconds = timeoutSeconds;
        this.clientInterceptor = clientInterceptor;
        Map<String, ManagedChannel> channels = new HashMap<>();
        for (String server : servers) {
            channels.put(server, buildChannel(server));
        }
        this.snapshot = new ServerSnapshot(servers, channels);
    }

    public IndexingServiceClient(List<String> servers) {
        this(servers, DEFAULT_TIMEOUT_SECONDS);
    }

    private ManagedChannel buildChannel(String server) {
        ManagedChannelBuilder<?> b = ManagedChannelBuilder.forTarget(server)
                .usePlaintext()
                .keepAliveTime(300, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS);
        if (clientInterceptor != null) {
            b.intercept(clientInterceptor);
        }
        return b.build();
    }

    @Override
    public synchronized void updateServers(List<String> newServers) {
        ServerSnapshot current = this.snapshot;

        // Compute diff
        Set<String> added = new LinkedHashSet<>(newServers);
        added.removeAll(current.channels.keySet());

        Set<String> removed = new LinkedHashSet<>(current.channels.keySet());
        removed.removeAll(new HashSet<>(newServers));

        // Build new channels map: reuse existing, add new
        Map<String, ManagedChannel> newChannels = new HashMap<>();
        for (String server : newServers) {
            ManagedChannel existing = current.channels.get(server);
            if (existing != null) {
                newChannels.put(server, existing);
            } else {
                newChannels.put(server, buildChannel(server));
            }
        }

        this.snapshot = new ServerSnapshot(newServers, newChannels);

        LOGGER.log(Level.INFO, "Updated indexing service servers: {0} (added: {1}, removed: {2})",
                new Object[]{newServers, added, removed});

        // Gracefully shutdown removed channels in background
        if (!removed.isEmpty()) {
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
    }

    /**
     * Searches all IndexingService instances and merges results by score.
     * If only one instance exists and returnScore is false, results are passed through.
     */
    public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                  float[] vector, int limit) {
        ServerSnapshot s = this.snapshot;

        if (s.servers.isEmpty()) {
            throw new RuntimeException("No indexing service instances available");
        }

        boolean multiInstance = s.servers.size() > 1;
        boolean returnScore = multiInstance; // always request scores when merging multiple instances

        LOGGER.log(Level.INFO, "client search: tablespace={0}, table={1}, index={2}, limit={3}, vectorDim={4}, instances={5}",
                new Object[]{tablespace, table, index, limit, vector.length, s.servers.size()});
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

        if (!multiInstance) {
            // Single instance: pass through
            ManagedChannel channel = s.channels.values().iterator().next();
            IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                    IndexingServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
            SearchResponse response = stub.search(request);
            List<Map.Entry<Bytes, Float>> results = toEntryList(response);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.INFO, "client search completed (single instance): index={0}, {1} results in {2} ms",
                    new Object[]{index, results.size(), elapsedMs});
            return results;
        }

        // Multiple instances: fan out, merge by score
        List<Map.Entry<Bytes, Float>> merged = new ArrayList<>();
        for (Map.Entry<String, ManagedChannel> entry : s.channels.entrySet()) {
            try {
                IndexingServiceGrpc.IndexingServiceBlockingStub stub =
                        IndexingServiceGrpc.newBlockingStub(entry.getValue())
                                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
                SearchResponse response = stub.search(request);
                merged.addAll(toEntryList(response));
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Search failed on instance " + entry.getKey(), e);
            }
        }

        // Re-rank by score descending and limit
        merged.sort(Comparator.<Map.Entry<Bytes, Float>, Float>comparing(Map.Entry::getValue).reversed());
        if (merged.size() > limit) {
            merged = merged.subList(0, limit);
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        LOGGER.log(Level.INFO, "client search completed (multi-instance fan-out): index={0}, {1} results in {2} ms",
                new Object[]{index, merged.size(), elapsedMs});
        return merged;
    }

    @Override
    public RemoteVectorIndexService.IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
        ServerSnapshot s = this.snapshot;

        if (s.servers.isEmpty()) {
            throw new RuntimeException("No indexing service instances available");
        }

        // Query the first available instance
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
                        resp.getLastLsnLedger(), resp.getLastLsnOffset(),
                        resp.getStatus());
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "GetIndexStatus failed on instance " + entry.getKey(), e);
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
    private static final long CATCHUP_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

    @Override
    public void waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber) throws InterruptedException {
        ServerSnapshot s = this.snapshot;
        for (Map.Entry<String, ManagedChannel> serverEntry : s.channels.entrySet()) {
            String server = serverEntry.getKey();
            ManagedChannel channel = serverEntry.getValue();
            waitForInstanceCatchUp(server, channel, tablespace, sequenceNumber);
        }
    }

    private void waitForInstanceCatchUp(String server, ManagedChannel channel,
                                         String tablespace,
                                         LogSequenceNumber target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + CATCHUP_TIMEOUT_MS;
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
                LogSequenceNumber instanceLsn = new LogSequenceNumber(
                        resp.getLastLsnLedger(), resp.getLastLsnOffset());
                if (instanceLsn.after(target) || instanceLsn.equals(target)) {
                    LOGGER.log(Level.INFO, "Instance {0} caught up for tablespace {1} to {2} (at {3})",
                            new Object[]{server, tablespace, target, instanceLsn});
                    return;
                }
                LOGGER.log(Level.INFO, "Instance {0} at {1} for tablespace {2}, waiting for {3} (status: {4})",
                        new Object[]{server, instanceLsn, tablespace, target, resp.getStatus()});
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Instance {0} unreachable for tablespace {1}, retrying: {2}",
                        new Object[]{server, tablespace, e.getMessage()});
            }
            Thread.sleep(CATCHUP_POLL_INTERVAL_MS);
        }
        LOGGER.log(Level.WARNING, "Instance {0} did not catch up for tablespace {1} to {2} within timeout",
                new Object[]{server, tablespace, target});
    }

    public List<String> getServers() {
        return snapshot.servers;
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
