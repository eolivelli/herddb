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

package herddb.remote;

import herddb.network.Channel;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.ReadResult;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Stateless dispatcher that turns inbound file-server PDUs into
 * {@link ObjectStorage} calls and writes the response PDU back on the
 * caller {@link Channel}. Issue #425 replaced the previous gRPC service
 * implementation with this class so the file server speaks the same
 * length-prefixed Netty wire protocol used by the rest of HerdDB.
 *
 * <p>Each handler dispatches its storage call onto the read or write
 * executor lane (issue #100) so checkpoint write-paths cannot starve
 * read-paths and vice versa.
 *
 * <p>The handlers take ownership of the inbound {@link Pdu} and release
 * it once the request payload has been parsed (or, for async paths, once
 * the retained payload slice has been extracted). The caller of
 * {@link #handle(Pdu, Channel)} is therefore <em>not</em> responsible for
 * closing the PDU.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceImpl {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceImpl.class.getName());

    private final ObjectStorage storage;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    private final Counter writeRequests;
    private final Counter writeErrors;
    private final Counter writtenBytes;
    private final OpStatsLogger writeLatency;

    private final Counter readRequests;
    private final Counter readErrors;
    private final Counter readNotFound;
    private final Counter readBytes;
    private final OpStatsLogger readLatency;

    private final Counter deleteRequests;
    private final Counter deleteErrors;
    private final OpStatsLogger deleteLatency;

    private final Counter deleteFilesRequests;
    private final Counter deleteFilesErrors;
    private final Counter deleteFilesPathsTotal;
    private final Counter deleteFilesPathsDeleted;
    private final OpStatsLogger deleteFilesLatency;

    private final Counter listRequests;
    private final Counter listErrors;
    private final OpStatsLogger listLatency;

    private final Counter deleteByPrefixRequests;
    private final Counter deleteByPrefixErrors;
    private final OpStatsLogger deleteByPrefixLatency;

    private final Counter writeBlockRequests;
    private final Counter writeBlockErrors;
    private final Counter writtenBlockBytes;
    private final OpStatsLogger writeBlockLatency;

    private final Counter readRangeRequests;
    private final Counter readRangeErrors;
    private final Counter readRangeNotFound;
    private final Counter readRangeBytes;
    private final OpStatsLogger readRangeLatency;

    public RemoteFileServiceImpl(ObjectStorage storage) {
        this(storage, NullStatsLogger.INSTANCE, null, null);
    }

    public RemoteFileServiceImpl(ObjectStorage storage, StatsLogger statsLogger) {
        this(storage, statsLogger, null, null);
    }

    /**
     * Creates the service with dedicated read/write execution lanes (issue #100).
     * Each PDU handler dispatches its storage call and its completion callback
     * onto {@code readExecutor} or {@code writeExecutor} according to
     * operation type, so slow write-path callbacks cannot starve read-path
     * responses. If either executor is {@code null}, handlers fall back to
     * the storage future's default thread.
     */
    public RemoteFileServiceImpl(ObjectStorage storage, StatsLogger statsLogger,
                                 Executor readExecutor, Executor writeExecutor) {
        this.storage = storage;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;

        StatsLogger scope = statsLogger.scope("rfs");

        this.writeRequests = scope.getCounter("write_requests");
        this.writeErrors = scope.getCounter("write_errors");
        this.writtenBytes = scope.getCounter("written_bytes");
        this.writeLatency = scope.getOpStatsLogger("write_latency");

        this.readRequests = scope.getCounter("read_requests");
        this.readErrors = scope.getCounter("read_errors");
        this.readNotFound = scope.getCounter("read_not_found");
        this.readBytes = scope.getCounter("read_bytes");
        this.readLatency = scope.getOpStatsLogger("read_latency");

        this.deleteRequests = scope.getCounter("delete_requests");
        this.deleteErrors = scope.getCounter("delete_errors");
        this.deleteLatency = scope.getOpStatsLogger("delete_latency");

        this.deleteFilesRequests = scope.getCounter("deletefiles_requests");
        this.deleteFilesErrors = scope.getCounter("deletefiles_errors");
        this.deleteFilesPathsTotal = scope.getCounter("deletefiles_paths_total");
        this.deleteFilesPathsDeleted = scope.getCounter("deletefiles_paths_deleted");
        this.deleteFilesLatency = scope.getOpStatsLogger("deletefiles_latency");

        this.listRequests = scope.getCounter("list_requests");
        this.listErrors = scope.getCounter("list_errors");
        this.listLatency = scope.getOpStatsLogger("list_latency");

        this.deleteByPrefixRequests = scope.getCounter("deletebyprefix_requests");
        this.deleteByPrefixErrors = scope.getCounter("deletebyprefix_errors");
        this.deleteByPrefixLatency = scope.getOpStatsLogger("deletebyprefix_latency");

        this.writeBlockRequests = scope.getCounter("writeblock_requests");
        this.writeBlockErrors = scope.getCounter("writeblock_errors");
        this.writtenBlockBytes = scope.getCounter("writeblock_bytes");
        this.writeBlockLatency = scope.getOpStatsLogger("writeblock_latency");

        this.readRangeRequests = scope.getCounter("readrange_requests");
        this.readRangeErrors = scope.getCounter("readrange_errors");
        this.readRangeNotFound = scope.getCounter("readrange_not_found");
        this.readRangeBytes = scope.getCounter("readrange_bytes");
        this.readRangeLatency = scope.getOpStatsLogger("readrange_latency");
    }

    /**
     * Dispatches an inbound file-server PDU. Returns {@code true} if the type
     * was recognized (and the PDU was consumed by this dispatcher), {@code false}
     * otherwise. When this method returns {@code true} the caller must NOT
     * close the PDU — the handler has already taken ownership.
     */
    public boolean handle(Pdu pdu, Channel channel) {
        switch (pdu.type) {
            case Pdu.TYPE_FS_WRITE_FILE:
                handleWriteFile(pdu, channel);
                return true;
            case Pdu.TYPE_FS_WRITE_FILE_BLOCK:
                handleWriteFileBlock(pdu, channel);
                return true;
            case Pdu.TYPE_FS_READ_FILE:
                handleReadFile(pdu, channel);
                return true;
            case Pdu.TYPE_FS_READ_FILE_RANGE:
                handleReadFileRange(pdu, channel);
                return true;
            case Pdu.TYPE_FS_DELETE_FILE:
                handleDeleteFile(pdu, channel);
                return true;
            case Pdu.TYPE_FS_DELETE_FILES:
                handleDeleteFiles(pdu, channel);
                return true;
            case Pdu.TYPE_FS_LIST_FILES:
                handleListFiles(pdu, channel);
                return true;
            case Pdu.TYPE_FS_DELETE_BY_PREFIX:
                handleDeleteByPrefix(pdu, channel);
                return true;
            default:
                return false;
        }
    }

    // ----------------------------------------------------------------------
    // WRITE_FILE
    // ----------------------------------------------------------------------

    private void handleWriteFile(Pdu pdu, Channel channel) {
        // Parse on the calling (Netty) thread, then hop to the write lane.
        // The retained slice keeps the inbound buffer alive past pdu.close().
        final long messageId = pdu.messageId;
        final String path;
        final ByteBuf content;
        try {
            path = PduCodec.WriteFileRequest.readPath(pdu);
            content = PduCodec.WriteFileRequest.readContent(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse can fail on truncated / malformed frames;
            // the netty thread must not die without replying to the client.
            pdu.close();
            sendError(channel, messageId, "malformed WRITE_FILE: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(writeExecutor, () -> writeFileImpl(messageId, path, content, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: writeExecutor may reject the task during shutdown
            // (RejectedExecutionException) or when the queue is bounded and
            // saturated. Either way the lane lambda will not run, so the
            // retained content slice would leak. Release it here, then surface
            // the failure to the client.
            ReferenceCountUtil.safeRelease(content);
            sendError(channel, messageId,
                    "writeFile dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void writeFileImpl(long messageId, String path, ByteBuf content, Channel channel) {
        long start = System.nanoTime();
        writeRequests.inc();
        // ObjectStorage.write takes byte[] — copy out of the retained slice.
        // The slice is released after the async write completes.
        final int len = content.readableBytes();
        final byte[] bytes = new byte[len];
        content.getBytes(content.readerIndex(), bytes);
        attachCallback(storage.write(path, bytes), writeExecutor, (v, t) -> {
            try {
                long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                if (t != null) {
                    writeErrors.inc();
                    writeLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.SEVERE, "writeFile failed for path " + path, t);
                    sendError(channel, messageId, "writeFile failed: " + t.getMessage());
                } else {
                    writtenBytes.addCount(len);
                    writeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.FINE, "writeFile path={0} size={1} time={2}ms",
                            new Object[]{path, len, elapsedMs(start)});
                    channel.sendReplyMessage(messageId,
                            PduCodec.WriteFileResponse.write(messageId, len));
                }
            } finally {
                ReferenceCountUtil.safeRelease(content);
            }
        });
    }

    // ----------------------------------------------------------------------
    // WRITE_FILE_BLOCK
    // ----------------------------------------------------------------------

    private void handleWriteFileBlock(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String path;
        final long blockIndex;
        final ByteBuf content;
        try {
            path = PduCodec.WriteFileBlockRequest.readPath(pdu);
            blockIndex = PduCodec.WriteFileBlockRequest.readBlockIndex(pdu);
            content = PduCodec.WriteFileBlockRequest.readContent(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse can fail on truncated / malformed frames.
            pdu.close();
            sendError(channel, messageId, "malformed WRITE_FILE_BLOCK: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(writeExecutor, () -> writeFileBlockImpl(messageId, path, blockIndex, content, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: writeExecutor may reject the task at shutdown or
            // under saturation; release the retained slice that the lambda
            // would have owned to avoid a direct-memory leak.
            ReferenceCountUtil.safeRelease(content);
            sendError(channel, messageId,
                    "writeFileBlock dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void writeFileBlockImpl(long messageId, String path, long blockIndex,
                                    ByteBuf content, Channel channel) {
        long start = System.nanoTime();
        writeBlockRequests.inc();
        final int len = content.readableBytes();
        final byte[] bytes = new byte[len];
        content.getBytes(content.readerIndex(), bytes);
        attachCallback(storage.writeBlock(path, blockIndex, bytes), writeExecutor, (v, t) -> {
            try {
                long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                if (t != null) {
                    writeBlockErrors.inc();
                    writeBlockLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.SEVERE, "writeFileBlock failed for path " + path
                            + " block " + blockIndex, t);
                    sendError(channel, messageId, "writeFileBlock failed: " + t.getMessage());
                } else {
                    writtenBlockBytes.addCount(len);
                    writeBlockLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.FINE, "writeFileBlock path={0} block={1} size={2} time={3}ms",
                            new Object[]{path, blockIndex, len, elapsedMs(start)});
                    channel.sendReplyMessage(messageId,
                            PduCodec.WriteFileBlockResponse.write(messageId, len));
                }
            } finally {
                ReferenceCountUtil.safeRelease(content);
            }
        });
    }

    // ----------------------------------------------------------------------
    // READ_FILE
    // ----------------------------------------------------------------------

    private void handleReadFile(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String path;
        try {
            path = PduCodec.ReadFileRequest.readPath(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed READ_FILE: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(readExecutor, () -> readFileImpl(messageId, path, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: readExecutor may reject the task during shutdown
            // or under saturation. Surface as ERROR so the client times out
            // immediately instead of waiting for the configured timeout.
            sendError(channel, messageId,
                    "readFile dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void readFileImpl(long messageId, String path, Channel channel) {
        long start = System.nanoTime();
        readRequests.inc();
        attachCallback(storage.read(path), readExecutor, (result, t) -> {
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            try {
                if (t != null) {
                    readErrors.inc();
                    readLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.SEVERE, "readFile failed for path " + path, t);
                    sendError(channel, messageId, "readFile failed: " + t.getMessage());
                } else if (result.status() == ReadResult.Status.NOT_FOUND) {
                    readNotFound.inc();
                    readLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.INFO, "readFile path={0} found=false time={1}ms",
                            new Object[]{path, elapsedMs(start)});
                    channel.sendReplyMessage(messageId, PduCodec.ReadFileResponse.writeNotFound(messageId));
                } else {
                    ByteBuf byteBuf = result.byteBuf();
                    int contentLength = byteBuf.readableBytes();
                    readBytes.addCount(contentLength);
                    readLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.FINE, "readFile path={0} size={1} time={2}ms",
                            new Object[]{path, contentLength, elapsedMs(start)});
                    // writeFound copies the bytes into a fresh response buffer;
                    // result.release() below frees the storage-side buffer.
                    channel.sendReplyMessage(messageId,
                            PduCodec.ReadFileResponse.writeFound(messageId, byteBuf));
                }
            } finally {
                if (result != null) {
                    result.release();
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // READ_FILE_RANGE
    // ----------------------------------------------------------------------

    private void handleReadFileRange(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String path;
        final long offset;
        final int length;
        final int blockSize;
        try {
            path = PduCodec.ReadFileRangeRequest.readPath(pdu);
            offset = PduCodec.ReadFileRangeRequest.readOffset(pdu);
            length = PduCodec.ReadFileRangeRequest.readLength(pdu);
            blockSize = PduCodec.ReadFileRangeRequest.readBlockSize(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed READ_FILE_RANGE: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(readExecutor, () -> readFileRangeImpl(messageId, path, offset, length, blockSize, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: readExecutor may reject the task during shutdown
            // or under saturation; surface as ERROR so the client fails fast.
            sendError(channel, messageId,
                    "readFileRange dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void readFileRangeImpl(long messageId, String path, long offset, int length,
                                   int blockSize, Channel channel) {
        long start = System.nanoTime();
        readRangeRequests.inc();
        attachCallback(storage.readRange(path, offset, length, blockSize), readExecutor, (result, t) -> {
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            if (t != null) {
                readRangeErrors.inc();
                readRangeLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.SEVERE, "readFileRange failed for path " + path, t);
                sendError(channel, messageId, "readFileRange failed: " + t.getMessage());
                return;
            }
            try {
                if (result.status() == ReadResult.Status.NOT_FOUND) {
                    readRangeNotFound.inc();
                    readRangeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    channel.sendReplyMessage(messageId, PduCodec.ReadFileRangeResponse.writeNotFound(messageId));
                } else {
                    ByteBuf byteBuf = result.byteBuf();
                    int contentLength = byteBuf.readableBytes();
                    readRangeBytes.addCount(contentLength);
                    readRangeLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    LOGGER.log(Level.FINE, "readFileRange path={0} offset={1} size={2} time={3}ms",
                            new Object[]{path, offset, contentLength, elapsedMs(start)});
                    channel.sendReplyMessage(messageId,
                            PduCodec.ReadFileRangeResponse.writeFound(messageId, byteBuf));
                }
            } finally {
                result.release();
            }
        });
    }

    // ----------------------------------------------------------------------
    // DELETE_FILE
    // ----------------------------------------------------------------------

    private void handleDeleteFile(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String path;
        try {
            path = PduCodec.DeleteFileRequest.readPath(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed DELETE_FILE: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(writeExecutor, () -> deleteFileImpl(messageId, path, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: writeExecutor may reject the task during shutdown.
            sendError(channel, messageId,
                    "deleteFile dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void deleteFileImpl(long messageId, String path, Channel channel) {
        long start = System.nanoTime();
        deleteRequests.inc();
        attachCallback(storage.deleteLogical(path), writeExecutor, (deleted, t) -> {
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            if (t != null) {
                deleteErrors.inc();
                deleteLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.SEVERE, "deleteFile failed for path " + path, t);
                sendError(channel, messageId, "deleteFile failed: " + t.getMessage());
            } else {
                deleteLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.INFO, "deleteFile path={0} deleted={1} time={2}ms",
                        new Object[]{path, deleted, elapsedMs(start)});
                channel.sendReplyMessage(messageId,
                        PduCodec.DeleteFileResponse.write(messageId, Boolean.TRUE.equals(deleted)));
            }
        });
    }

    // ----------------------------------------------------------------------
    // DELETE_FILES (batch)
    // ----------------------------------------------------------------------

    private void handleDeleteFiles(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final List<String> paths;
        try {
            paths = PduCodec.DeleteFilesRequest.readPaths(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed DELETE_FILES: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(writeExecutor, () -> deleteFilesImpl(messageId, paths, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: writeExecutor may reject the task during shutdown.
            sendError(channel, messageId,
                    "deleteFiles dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void deleteFilesImpl(long messageId, List<String> paths, Channel channel) {
        long start = System.nanoTime();
        deleteFilesRequests.inc();
        int total = paths.size();
        deleteFilesPathsTotal.addCount(total);
        if (total == 0) {
            deleteFilesLatency.registerSuccessfulEvent(
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start), TimeUnit.MICROSECONDS);
            channel.sendReplyMessage(messageId,
                    PduCodec.DeleteFilesResponse.write(messageId, java.util.Collections.emptyList()));
            return;
        }
        // Pre-size the outcomes list so each callback writes back to its own slot.
        // Per-path completion order is unpredictable but each slot is written once.
        final PduCodec.DeleteFilesResponse.Outcome[] outcomes =
                new PduCodec.DeleteFilesResponse.Outcome[total];
        AtomicInteger remaining = new AtomicInteger(total);
        AtomicInteger deletedTrue = new AtomicInteger();
        AtomicInteger deletedFalse = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        for (int i = 0; i < total; i++) {
            final int idx = i;
            final String path = paths.get(i);
            attachCallback(storage.deleteLogical(path), writeExecutor, (deleted, t) -> {
                if (t != null) {
                    errors.incrementAndGet();
                    String msg = t.getMessage();
                    outcomes[idx] = new PduCodec.DeleteFilesResponse.Outcome(path, false,
                            msg == null ? t.getClass().getSimpleName() : msg);
                } else if (Boolean.TRUE.equals(deleted)) {
                    deletedTrue.incrementAndGet();
                    outcomes[idx] = new PduCodec.DeleteFilesResponse.Outcome(path, true, "");
                } else {
                    deletedFalse.incrementAndGet();
                    outcomes[idx] = new PduCodec.DeleteFilesResponse.Outcome(path, false, "");
                }
                if (remaining.decrementAndGet() == 0) {
                    finishDeleteFiles(messageId, start, total,
                            deletedTrue.get(), deletedFalse.get(), errors.get(),
                            outcomes, channel);
                }
            });
        }
    }

    private void finishDeleteFiles(long messageId, long start, int total,
                                   int deletedTrue, int deletedFalse, int errors,
                                   PduCodec.DeleteFilesResponse.Outcome[] outcomes, Channel channel) {
        long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        long elapsedMs = elapsedMs(start);
        deleteFilesPathsDeleted.addCount(deletedTrue);
        if (errors > 0) {
            deleteFilesErrors.inc();
            deleteFilesLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
        } else {
            deleteFilesLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
        }
        LOGGER.log(Level.INFO,
                "deleteFiles count={0} deletedTrue={1} deletedFalse={2} errors={3} time={4}ms",
                new Object[]{total, deletedTrue, deletedFalse, errors, elapsedMs});
        List<PduCodec.DeleteFilesResponse.Outcome> list = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            list.add(outcomes[i]);
        }
        channel.sendReplyMessage(messageId,
                PduCodec.DeleteFilesResponse.write(messageId, list));
    }

    // ----------------------------------------------------------------------
    // LIST_FILES
    // ----------------------------------------------------------------------

    private void handleListFiles(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String prefix;
        try {
            prefix = PduCodec.ListFilesRequest.readPrefix(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed LIST_FILES: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(readExecutor, () -> listFilesImpl(messageId, prefix, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: readExecutor may reject the task during shutdown.
            sendError(channel, messageId,
                    "listFiles dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void listFilesImpl(long messageId, String prefix, Channel channel) {
        long start = System.nanoTime();
        listRequests.inc();
        attachCallback(storage.listLogical(prefix), readExecutor, (paths, t) -> {
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            if (t != null) {
                listErrors.inc();
                listLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.SEVERE, "listFiles failed for prefix " + prefix, t);
                sendError(channel, messageId, "listFiles failed: " + t.getMessage());
            } else {
                listLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.INFO, "listFiles prefix={0} count={1} time={2}ms",
                        new Object[]{prefix, paths.size(), elapsedMs(start)});
                channel.sendReplyMessage(messageId,
                        PduCodec.ListFilesResponse.write(messageId, paths));
            }
        });
    }

    // ----------------------------------------------------------------------
    // DELETE_BY_PREFIX
    // ----------------------------------------------------------------------

    private void handleDeleteByPrefix(Pdu pdu, Channel channel) {
        final long messageId = pdu.messageId;
        final String prefix;
        try {
            prefix = PduCodec.DeleteByPrefixRequest.readPrefix(pdu);
        } catch (RuntimeException parseError) {
            // Broad catch: PDU parse on malformed / truncated frames.
            pdu.close();
            sendError(channel, messageId, "malformed DELETE_BY_PREFIX: " + parseError.getMessage());
            return;
        }
        pdu.close();

        try {
            runOnLane(writeExecutor, () -> deleteByPrefixImpl(messageId, prefix, channel));
        } catch (RuntimeException dispatchError) {
            // Broad catch: writeExecutor may reject the task during shutdown.
            sendError(channel, messageId,
                    "deleteByPrefix dispatch failed: " + dispatchError.getMessage());
        }
    }

    private void deleteByPrefixImpl(long messageId, String prefix, Channel channel) {
        long start = System.nanoTime();
        deleteByPrefixRequests.inc();
        attachCallback(storage.deleteByPrefix(prefix), writeExecutor, (count, t) -> {
            long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            if (t != null) {
                deleteByPrefixErrors.inc();
                deleteByPrefixLatency.registerFailedEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.SEVERE, "deleteByPrefix failed for prefix " + prefix, t);
                sendError(channel, messageId, "deleteByPrefix failed: " + t.getMessage());
            } else {
                deleteByPrefixLatency.registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                LOGGER.log(Level.INFO, "deleteByPrefix prefix={0} deletedCount={1} time={2}ms",
                        new Object[]{prefix, count, elapsedMs(start)});
                channel.sendReplyMessage(messageId,
                        PduCodec.DeleteByPrefixResponse.write(messageId, count));
            }
        });
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static void sendError(Channel channel, long messageId, String message) {
        channel.sendReplyMessage(messageId, PduCodec.ErrorResponse.write(messageId, message));
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /**
     * Executes {@code task} on the given lane executor, or inline if the
     * executor is {@code null} (legacy single-lane mode).
     */
    private static void runOnLane(Executor lane, Runnable task) {
        if (lane != null) {
            lane.execute(task);
        } else {
            task.run();
        }
    }

    /**
     * Attaches {@code callback} to {@code future} using {@code executor} when
     * non-null ({@code whenCompleteAsync}), otherwise falls back to
     * {@code whenComplete}.
     */
    private static <T> void attachCallback(CompletableFuture<T> future,
                                           Executor executor,
                                           BiConsumer<? super T, ? super Throwable> callback) {
        if (executor != null) {
            future.whenCompleteAsync(callback, executor);
        } else {
            future.whenComplete(callback);
        }
    }
}
