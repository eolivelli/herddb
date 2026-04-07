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

import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link WatermarkStore} that persists the last-committed checkpoint LSN on remote
 * storage (S3 via the remote file service) so that an indexing service pod with an
 * ephemeral local disk can resume from the correct position after a restart.
 *
 * <p><b>Save contract</b>: per the {@link WatermarkStore} interface, this store is
 * only called after a matching {@code indexCheckpoint} has fully completed (all
 * pages + IndexStatus + {@code _metadata/latest.checkpoint} committed to S3). This
 * keeps S3 watermark writes bounded to ≤ 1 per checkpoint (low cost / rate), and
 * guarantees that loading the watermark on a wiped disk always points at a
 * consistent set of S3 checkpoint markers.
 *
 * <p>Object layout on S3:
 * <pre>
 *   {tableSpace}/_indexing/{instanceId}/watermark.lsn
 * </pre>
 * The {@code instanceId} segment keeps multi-instance shards from clobbering each
 * other. Format: {@code version | flags | ledgerId | offset | XXHash64}.
 *
 * <p>Monotonicity: {@link #save(LogSequenceNumber)} rejects (no-op) any LSN
 * strictly before the currently-published one, so a stray concurrent writer cannot
 * regress progress.
 *
 * @author enrico.olivelli
 */
public class S3WatermarkStore implements WatermarkStore {

    private static final Logger LOGGER = Logger.getLogger(S3WatermarkStore.class.getName());

    /**
     * Abstraction over {@link herddb.remote.RemoteFileServiceClient} so this class
     * can be unit-tested without pulling in a full gRPC client. The indexing-service
     * module does not depend directly on herddb-remote-file-service; the concrete
     * client is bridged in via {@link IndexingServer} (reflectively).
     */
    public interface RemoteFileIO {
        void writeFile(String path, byte[] content) throws IOException;

        /**
         * @return file bytes, or {@code null} if absent
         */
        byte[] readFile(String path) throws IOException;
    }

    private final RemoteFileIO io;
    private final String path;

    public S3WatermarkStore(RemoteFileIO io, String tableSpace, int instanceId) {
        this.io = io;
        this.path = tableSpace + "/_indexing/" + instanceId + "/watermark.lsn";
    }

    /** Exposed for tests. */
    public String getPath() {
        return path;
    }

    @Override
    public LogSequenceNumber load() throws IOException {
        byte[] data;
        try {
            data = io.readFile(path);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Failed to read watermark from " + path, e);
        }
        if (data == null) {
            LOGGER.log(Level.INFO, "No watermark found at {0}, starting from beginning", path);
            return LogSequenceNumber.START_OF_TIME;
        }
        if (data.length < 8) {
            throw new CorruptWatermarkException(
                    "watermark object at " + path + " is too short: " + data.length + " bytes");
        }
        try {
            XXHash64Utils.verifyBlockWithFooter(data, 0, data.length);
        } catch (Exception e) {
            throw new CorruptWatermarkException("watermark checksum mismatch at " + path, e);
        }
        try (SimpleByteArrayInputStream in = new SimpleByteArrayInputStream(data);
             ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
            long version = din.readVLong();
            long flags = din.readVLong();
            if (version != 1 || flags != 0) {
                throw new CorruptWatermarkException(
                        "unsupported watermark version/flags at " + path
                                + ": version=" + version + ", flags=" + flags);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            LogSequenceNumber lsn = new LogSequenceNumber(ledgerId, offset);
            LOGGER.log(Level.INFO, "Loaded watermark from {0}: {1}", new Object[]{path, lsn});
            return lsn;
        }
    }

    @Override
    public void save(LogSequenceNumber lsn) throws IOException {
        // Enforce monotonicity: never move the watermark backwards.
        LogSequenceNumber current;
        try {
            current = load();
        } catch (CorruptWatermarkException e) {
            // If existing object is corrupt, overwrite it — the new save is
            // the authoritative source once published (and the in-memory
            // processing state shows 'lsn' really was checkpointed).
            LOGGER.log(Level.WARNING, "Existing watermark is corrupt; overwriting: {0}", e.getMessage());
            current = LogSequenceNumber.START_OF_TIME;
        }
        if (current.after(lsn)) {
            LOGGER.log(Level.WARNING,
                    "Refusing to save watermark {0}: regresses from {1}",
                    new Object[]{lsn, current});
            return;
        }

        VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
        XXHash64Utils.HashingOutputStream hashOut = new XXHash64Utils.HashingOutputStream(bos);
        try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(hashOut)) {
            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags
            dout.writeZLong(lsn.ledgerId);
            dout.writeZLong(lsn.offset);
            dout.writeLong(hashOut.hash());
        }
        try {
            io.writeFile(path, bos.toByteArray());
        } catch (RuntimeException e) {
            throw new IOException("Failed to write watermark to " + path, e);
        }
        LOGGER.log(Level.FINE, "Saved watermark to {0}: {1}", new Object[]{path, lsn});
    }

    /** Thrown when a stored watermark fails integrity checks. */
    public static class CorruptWatermarkException extends IOException {
        private static final long serialVersionUID = 1L;
        public CorruptWatermarkException(String message) {
            super(message);
        }
        public CorruptWatermarkException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
