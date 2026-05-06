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

package herddb.remote.admin;

import herddb.network.Channel;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.remote.RemoteFileServer;
import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.InMemoryBlockCacheObjectStorage;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * File-server admin dispatcher (issue #336, ported to native Netty in
 * issue #425). Hosted on the same Netty channel as
 * {@link herddb.remote.RemoteFileServiceImpl}; PDU type-byte multiplexing
 * routes inbound messages to the right dispatcher.
 *
 * <p>Two operations:
 * <ul>
 *   <li>{@code GET_SERVER_INFO} — returns a snapshot of JVM, disk-cache, and
 *       block-cache stats. Lock-free reads; safe to call any time.</li>
 *   <li>{@code RESIZE_DISK_CACHE} — calls
 *       {@link CachingObjectStorage#setMaxCacheBytes(long)} to resize the
 *       disk-LRU on the fly. Not persisted across restarts.</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class RemoteFileServerAdminImpl {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServerAdminImpl.class.getName());

    private final RemoteFileServer server;

    public RemoteFileServerAdminImpl(RemoteFileServer server) {
        this.server = server;
    }

    /**
     * Dispatches an inbound admin PDU. Returns {@code true} if the type was
     * recognized (and the PDU was consumed), {@code false} otherwise. When
     * this method returns {@code true} the caller must NOT close the PDU.
     */
    public boolean handle(Pdu pdu, Channel channel) {
        switch (pdu.type) {
            case Pdu.TYPE_FS_GET_SERVER_INFO:
                handleGetServerInfo(pdu, channel);
                return true;
            case Pdu.TYPE_FS_RESIZE_DISK_CACHE:
                handleResizeDiskCache(pdu, channel);
                return true;
            default:
                return false;
        }
    }

    private void handleGetServerInfo(Pdu pdu, Channel channel) {
        long messageId = pdu.messageId;
        try {
            PduCodec.GetServerInfoResponse.Info info = new PduCodec.GetServerInfoResponse.Info();
            info.host = server.getHost();
            info.port = server.getPort();
            info.storageMode = server.getStorageMode();

            Runtime rt = Runtime.getRuntime();
            info.jvmHeapUsedBytes = rt.totalMemory() - rt.freeMemory();
            info.jvmHeapMaxBytes = rt.maxMemory();

            CachingObjectStorage dc = server.getDiskCache();
            if (dc != null) {
                com.github.benmanes.caffeine.cache.stats.CacheStats stats = dc.diskLruStats();
                info.diskCacheMaxBytes = dc.getMaxCacheBytes();
                info.diskCacheHitCount = stats.hitCount();
                info.diskCacheMissCount = stats.missCount();
                info.diskCacheEvictionCount = stats.evictionCount();
                info.diskCacheHitBytes = dc.getHitBytes();
                info.diskCacheMissBytes = dc.getMissBytes();
                info.diskCacheEstimatedEntries = dc.estimatedEntries();
            }

            InMemoryBlockCacheObjectStorage bc = server.getBlockCache();
            if (bc != null) {
                com.github.benmanes.caffeine.cache.stats.CacheStats bStats = bc.stats();
                info.blockCacheMaxBytes = bc.getMaxBytes();
                info.blockCacheEstimatedBytes = bc.estimatedBytes();
                info.blockCacheEstimatedEntries = bc.estimatedSize();
                info.blockCacheHits = bStats.hitCount();
                info.blockCacheMisses = bStats.missCount();
                info.blockCacheEvictions = bStats.evictionCount();
            }

            channel.sendReplyMessage(messageId,
                    PduCodec.GetServerInfoResponse.write(messageId, info));
        } catch (RuntimeException e) {
            // Broad catch: admin entry point — must not let an unchecked
            // exception kill the netty thread without replying to the client.
            LOGGER.log(Level.WARNING, "GetServerInfo failed", e);
            channel.sendReplyMessage(messageId,
                    PduCodec.ErrorResponse.write(messageId, "GetServerInfo failed: " + e.getMessage()));
        } finally {
            pdu.close();
        }
    }

    private void handleResizeDiskCache(Pdu pdu, Channel channel) {
        long messageId = pdu.messageId;
        try {
            CachingObjectStorage dc = server.getDiskCache();
            if (dc == null) {
                channel.sendReplyMessage(messageId,
                        PduCodec.ErrorResponse.write(messageId,
                                "disk cache not available: storage.mode is not s3"));
                return;
            }
            long newMax = PduCodec.ResizeDiskCacheRequest.readNewMaxBytes(pdu);
            if (newMax <= 0) {
                channel.sendReplyMessage(messageId,
                        PduCodec.ErrorResponse.write(messageId,
                                "new_max_bytes must be > 0, got " + newMax));
                return;
            }
            long prev = dc.setMaxCacheBytes(newMax);
            LOGGER.log(Level.INFO,
                    "ResizeDiskCache: previousMax={0} bytes, newMax={1} bytes",
                    new Object[]{prev, newMax});
            channel.sendReplyMessage(messageId,
                    PduCodec.ResizeDiskCacheResponse.write(messageId, prev, newMax));
        } catch (RuntimeException e) {
            // Broad catch: admin entry point — must not let an unchecked
            // exception kill the netty thread without replying to the client.
            LOGGER.log(Level.WARNING, "ResizeDiskCache failed", e);
            channel.sendReplyMessage(messageId,
                    PduCodec.ErrorResponse.write(messageId, "ResizeDiskCache failed: " + e.getMessage()));
        } finally {
            pdu.close();
        }
    }
}
