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
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link WatermarkStore} that persists the durable indexing-service checkpoint
 * state on remote storage (S3 via the remote file service) so that an indexing
 * service pod with an ephemeral local disk can resume from the correct state
 * after a restart.
 *
 * <p><b>Save contract</b>: per the {@link WatermarkStore} interface, this store is
 * only called after a matching {@code indexCheckpoint} has fully completed (all
 * pages + IndexStatus + {@code _metadata/latest.checkpoint} committed to S3). This
 * keeps S3 watermark writes bounded to ≤ 1 per checkpoint (low cost / rate), and
 * guarantees that loading the snapshot on a wiped disk always points at a
 * consistent set of S3 checkpoint markers.
 *
 * <p>Object layout on S3:
 * <pre>
 *   {tableSpace}/_indexing/{instanceId}/watermark.lsn
 * </pre>
 * The {@code instanceId} segment keeps multi-instance shards from clobbering each
 * other.
 *
 * <p>Binary format (version 1, flags 0):
 * <pre>
 *   version(VLong=1) | flags(VLong=0)
 *   ledgerId(ZLong) | offset(ZLong) | numInstances(VInt)
 *   tableCount(VInt)
 *   for each table:   VInt len, byte[len] serialised Table
 *   indexCount(VInt)
 *   for each index:   VInt len, byte[len] serialised Index
 *   XXHash64(Long, 8 bytes, covering all preceding bytes)
 * </pre>
 *
 * <p>Monotonicity: {@link #save(WatermarkSnapshot)} rejects (no-op) any LSN
 * strictly before the currently-published one, so a stray concurrent writer cannot
 * regress progress.
 *
 * @author enrico.olivelli
 */
public class S3WatermarkStore implements WatermarkStore {

    private static final Logger LOGGER = Logger.getLogger(S3WatermarkStore.class.getName());

    /**
     * Upper bound on the number of tables or indexes in a single watermark object.
     * The XXHash64 footer makes a corrupt count fail the hash check first, but these
     * bounds provide defense-in-depth against format-version skew that might produce
     * a hash-valid but semantically garbage payload.
     */
    private static final int MAX_TABLES_OR_INDEXES = 10_000;
    /** Upper bound on the serialized size of a single Table or Index blob. */
    private static final int MAX_BLOB_BYTES = 10 * 1024 * 1024; // 10 MiB

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
    public WatermarkSnapshot load() throws IOException {
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
            return WatermarkSnapshot.START_OF_TIME;
        }
        if (data.length < 8) {
            throw new CorruptWatermarkException(
                    "watermark object at " + path + " is too short: " + data.length + " bytes");
        }
        boolean hashValid;
        try {
            hashValid = XXHash64Utils.verifyBlockWithFooter(data, 0, data.length);
        } catch (RuntimeException e) {
            // verifyBlockWithFooter is a pure array operation; a RuntimeException here means
            // the data array is malformed (e.g. shorter than expected despite the length check).
            throw new CorruptWatermarkException("watermark checksum mismatch at " + path, e);
        }
        if (!hashValid) {
            throw new CorruptWatermarkException("watermark checksum mismatch at " + path);
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
            int numInstances = din.readVInt();

            int tableCount = din.readVInt();
            if (tableCount < 0 || tableCount > MAX_TABLES_OR_INDEXES) {
                throw new CorruptWatermarkException(
                        "watermark object at " + path + " is corrupt: tableCount=" + tableCount);
            }
            List<Table> tables = new ArrayList<>(tableCount);
            for (int i = 0; i < tableCount; i++) {
                int len = din.readVInt();
                if (len < 0 || len > MAX_BLOB_BYTES) {
                    throw new CorruptWatermarkException(
                            "watermark object at " + path + " is corrupt: table blob len=" + len
                                    + " at index " + i);
                }
                byte[] blob = new byte[len];
                din.readFully(blob);
                tables.add(Table.deserialize(blob));
            }
            int indexCount = din.readVInt();
            if (indexCount < 0 || indexCount > MAX_TABLES_OR_INDEXES) {
                throw new CorruptWatermarkException(
                        "watermark object at " + path + " is corrupt: indexCount=" + indexCount);
            }
            List<Index> vectorIndexes = new ArrayList<>(indexCount);
            for (int i = 0; i < indexCount; i++) {
                int len = din.readVInt();
                if (len < 0 || len > MAX_BLOB_BYTES) {
                    throw new CorruptWatermarkException(
                            "watermark object at " + path + " is corrupt: index blob len=" + len
                                    + " at index " + i);
                }
                byte[] blob = new byte[len];
                din.readFully(blob);
                vectorIndexes.add(Index.deserialize(blob));
            }

            WatermarkSnapshot snapshot = new WatermarkSnapshot(
                    new LogSequenceNumber(ledgerId, offset), numInstances,
                    tables, vectorIndexes);
            LOGGER.log(Level.INFO, "Loaded watermark from {0}: {1}", new Object[]{path, snapshot});
            return snapshot;
        }
    }

    @Override
    public void save(WatermarkSnapshot snapshot) throws IOException {
        // Enforce monotonicity: never move the watermark backwards.
        WatermarkSnapshot current;
        try {
            current = load();
        } catch (CorruptWatermarkException e) {
            // If existing object is corrupt, overwrite it — the new save is
            // the authoritative source once published (and the in-memory
            // processing state shows 'lsn' really was checkpointed).
            LOGGER.log(Level.WARNING, "Existing watermark is corrupt; overwriting: {0}", e.getMessage());
            current = WatermarkSnapshot.START_OF_TIME;
        }
        if (current.lsn.after(snapshot.lsn)) {
            LOGGER.log(Level.WARNING,
                    "Refusing to save watermark {0}: regresses from {1}",
                    new Object[]{snapshot, current});
            return;
        }

        VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
        XXHash64Utils.HashingOutputStream hashOut = new XXHash64Utils.HashingOutputStream(bos);
        try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(hashOut)) {
            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags
            dout.writeZLong(snapshot.lsn.ledgerId);
            dout.writeZLong(snapshot.lsn.offset);
            dout.writeVInt(snapshot.numInstances);
            // Schema snapshot: tables
            dout.writeVInt(snapshot.tables.size());
            for (Table t : snapshot.tables) {
                byte[] blob = t.serialize();
                dout.writeVInt(blob.length);
                dout.write(blob);
            }
            // Schema snapshot: vector indexes
            dout.writeVInt(snapshot.vectorIndexes.size());
            for (Index ix : snapshot.vectorIndexes) {
                byte[] blob = ix.serialize();
                dout.writeVInt(blob.length);
                dout.write(blob);
            }
            // Hash footer covers all preceding bytes (including schema).
            dout.writeLong(hashOut.hash());
        }
        try {
            io.writeFile(path, bos.toByteArray());
        } catch (RuntimeException e) {
            throw new IOException("Failed to write watermark to " + path, e);
        }
        LOGGER.log(Level.FINE, "Saved watermark to {0}: {1}", new Object[]{path, snapshot});
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
