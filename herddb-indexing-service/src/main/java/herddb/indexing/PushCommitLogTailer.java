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

import herddb.log.CommitLogTailing;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Testing-only {@link CommitLogTailing} implementation whose entries are
 * <em>pushed</em> in over the {@code PushEntries} gRPC RPC instead of being
 * tailed from a file or BookKeeper ledger.
 *
 * <p>Selected via {@code indexing.log.type=push}. In this mode the indexing
 * service needs neither a HerdDB server nor a materialised commit log: a test
 * client serializes {@link LogEntry} objects and pushes them straight into the
 * fixed-size in-memory buffer maintained here.
 *
 * <p>Threading model — identical contract to the other tailers:
 * <ul>
 *   <li>gRPC handler threads call {@link #push(LogSequenceNumber, LogEntry)},
 *       which blocks on the bounded buffer when it is full. This is the
 *       ingestion back-pressure: while the engine is stalled in a
 *       checkpoint/compaction the tailer thread stops draining, the buffer
 *       fills, and {@code push} parks the caller.</li>
 *   <li>A single tailer thread runs {@link #run()}, draining the buffer and
 *       dispatching each entry to the {@link EntryConsumer} (the engine's
 *       {@code processEntry}) — so the consumer keeps its single-threaded
 *       invariant, exactly as with {@code FileCommitLogTailer}.</li>
 * </ul>
 *
 * <p>Entries pushed at or before the current watermark are skipped, mirroring
 * {@code FileCommitLogTailer}: after a restart the tailer resumes at the
 * durable watermark and a client re-pushing already-applied entries is a safe
 * idempotent no-op. The client owns LSN assignment and is expected to push
 * entries in strictly increasing LSN order.
 *
 * @author enrico.olivelli
 */
public class PushCommitLogTailer implements CommitLogTailing {

    private static final Logger LOGGER = Logger.getLogger(PushCommitLogTailer.class.getName());

    /**
     * Poll granularity (ms) used on both the drain side ({@link #run()}) and
     * the push side ({@link #push}) so that {@link #close()} is observed
     * promptly without an unbounded park.
     */
    private static final long POLL_INTERVAL_MS = 200L;

    private final BlockingQueue<PushedEntry> buffer;
    private final EntryConsumer consumer;
    private final int bufferCapacity;

    private volatile LogSequenceNumber watermark;
    private volatile boolean running = true;
    /** Written only by the tailer thread; {@code volatile} for remote reads. */
    private volatile long entriesProcessed;

    /** Immutable (LSN, entry) pair held in the bounded buffer. */
    private static final class PushedEntry {
        final LogSequenceNumber lsn;
        final LogEntry entry;

        PushedEntry(LogSequenceNumber lsn, LogEntry entry) {
            this.lsn = lsn;
            this.entry = entry;
        }
    }

    /**
     * @param bufferCapacity fixed capacity of the in-memory buffer, in entries
     * @param startFrom      initial watermark — {@code START_OF_TIME} for a
     *                       fresh engine, or the durable recovery LSN after a
     *                       restart
     * @param consumer       the entry sink (the engine's {@code processEntry})
     */
    public PushCommitLogTailer(int bufferCapacity, LogSequenceNumber startFrom, EntryConsumer consumer) {
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("bufferCapacity must be >= 1, got " + bufferCapacity);
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer must not be null");
        }
        this.bufferCapacity = bufferCapacity;
        this.buffer = new ArrayBlockingQueue<>(bufferCapacity);
        this.watermark = startFrom != null ? startFrom : LogSequenceNumber.START_OF_TIME;
        this.consumer = consumer;
    }

    /**
     * Enqueues one entry, blocking while the fixed-size buffer is full. Called
     * by gRPC handler threads serving {@code PushEntries}; entries larger than
     * a single batch still drain safely because each entry is offered
     * individually.
     *
     * @throws InterruptedException  if the calling thread is interrupted while
     *                               parked on a full buffer
     * @throws IllegalStateException if the tailer has been closed
     */
    public void push(LogSequenceNumber lsn, LogEntry entry) throws InterruptedException {
        if (lsn == null || entry == null) {
            throw new IllegalArgumentException("lsn and entry must not be null");
        }
        PushedEntry pushed = new PushedEntry(lsn, entry);
        while (running) {
            if (buffer.offer(pushed, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
        throw new IllegalStateException(
                "PushCommitLogTailer is closed; entry at " + lsn + " rejected");
    }

    @Override
    public void run() {
        LOGGER.log(Level.INFO, "PushCommitLogTailer starting, watermark={0}, bufferCapacity={1}",
                new Object[]{watermark, bufferCapacity});
        while (running) {
            PushedEntry pushed;
            try {
                pushed = buffer.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (pushed == null) {
                continue;
            }
            // Idempotent skip: after a restart the tailer resumes at the
            // durable watermark and the client may re-push already-applied
            // entries. Mirrors FileCommitLogTailer.drainReader().
            if (!watermark.isStartOfTime() && !pushed.lsn.after(watermark)) {
                LOGGER.log(Level.FINE, "skipping pushed entry {0} at or before watermark {1}",
                        new Object[]{pushed.lsn, watermark});
                continue;
            }
            try {
                consumer.accept(pushed.lsn, pushed.entry);
            } catch (RuntimeException e) {
                // processEntry already swallows its own failures; this guard
                // keeps a pathological consumer error from killing the tailer.
                LOGGER.log(Level.SEVERE, "Error dispatching pushed entry at " + pushed.lsn, e);
            }
            watermark = pushed.lsn;
            entriesProcessed++;
        }
        LOGGER.log(Level.INFO, "PushCommitLogTailer stopped, watermark={0}, entriesProcessed={1}",
                new Object[]{watermark, entriesProcessed});
    }

    /** Number of entries currently buffered but not yet dispatched. */
    public int getBufferedCount() {
        return buffer.size();
    }

    /** Fixed capacity of the in-memory buffer, in entries. */
    public int getBufferCapacity() {
        return bufferCapacity;
    }

    @Override
    public LogSequenceNumber getWatermark() {
        return watermark;
    }

    @Override
    public long getEntriesProcessed() {
        return entriesProcessed;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        running = false;
    }
}
