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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link WatermarkStore} that keeps the watermark on the local filesystem under
 * {@code {dataDir}/watermark.dat}. Uses atomic write (tmp file + rename) for crash safety.
 *
 * <p>File format (version 1):
 * <pre>
 *   byte version=1
 *   long  ledgerId
 *   long  offset
 *   int   numInstances
 *   int   tableCount
 *   for each table:   int len, byte[len] serialised Table
 *   int   indexCount
 *   for each index:   int len, byte[len] serialised Index
 * </pre>
 *
 * <p>Older files that end after {@code numInstances} (written before the
 * schema-snapshot feature was added) are read gracefully: an {@link EOFException}
 * while attempting to read {@code tableCount} is caught and treated as an empty
 * schema, leaving the engine to rebuild its {@link SchemaTracker} by replaying
 * DDL entries from {@code START_OF_TIME}.
 *
 * <p>Suitable for deployments where the indexing service has a persistent local volume.
 * For ephemeral Kubernetes pods use {@link S3WatermarkStore} instead.
 *
 * @author enrico.olivelli
 */
public class LocalWatermarkStore implements WatermarkStore {

    private static final Logger LOGGER = Logger.getLogger(LocalWatermarkStore.class.getName());
    private static final String WATERMARK_FILE = "watermark.dat";
    private static final String WATERMARK_TMP_FILE = "watermark.dat.tmp";
    private static final byte FORMAT_VERSION = 1;

    private final Path dataDirectory;

    public LocalWatermarkStore(Path dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    @Override
    public WatermarkSnapshot load() throws IOException {
        Path watermarkFile = dataDirectory.resolve(WATERMARK_FILE);
        if (!Files.exists(watermarkFile)) {
            LOGGER.info("No watermark file found at " + watermarkFile + ", starting from beginning");
            return WatermarkSnapshot.START_OF_TIME;
        }
        try (InputStream fis = Files.newInputStream(watermarkFile);
                DataInputStream dis = new DataInputStream(fis)) {
            byte version = dis.readByte();
            if (version != FORMAT_VERSION) {
                throw new IOException("watermark file " + watermarkFile
                        + " has unsupported version " + version);
            }
            long ledgerId = dis.readLong();
            long offset = dis.readLong();
            int numInstances = dis.readInt();

            // Try to read the schema snapshot that follows numInstances in the
            // current format. Older watermark files end here and will throw
            // EOFException, which we catch and treat as an empty schema so the
            // engine falls back to replaying DDL entries from START_OF_TIME.
            List<Table> tables;
            List<Index> vectorIndexes;
            try {
                int tableCount = dis.readInt();
                tables = new ArrayList<>(tableCount);
                for (int i = 0; i < tableCount; i++) {
                    int len = dis.readInt();
                    byte[] blob = new byte[len];
                    dis.readFully(blob);
                    tables.add(Table.deserialize(blob));
                }
                int indexCount = dis.readInt();
                vectorIndexes = new ArrayList<>(indexCount);
                for (int i = 0; i < indexCount; i++) {
                    int len = dis.readInt();
                    byte[] blob = new byte[len];
                    dis.readFully(blob);
                    vectorIndexes.add(Index.deserialize(blob));
                }
            } catch (EOFException eof) {
                // Pre-schema watermark file: no schema data present.
                // The engine will start the tailer from START_OF_TIME to rebuild
                // its SchemaTracker by replaying DDL entries.
                tables = Collections.emptyList();
                vectorIndexes = Collections.emptyList();
            }

            WatermarkSnapshot snapshot = new WatermarkSnapshot(
                    new LogSequenceNumber(ledgerId, offset), numInstances,
                    tables, vectorIndexes);
            LOGGER.info("Loaded watermark: " + snapshot);
            return snapshot;
        }
    }

    @Override
    public void save(WatermarkSnapshot snapshot) throws IOException {
        Files.createDirectories(dataDirectory);
        Path tmpFile = dataDirectory.resolve(WATERMARK_TMP_FILE);
        Path watermarkFile = dataDirectory.resolve(WATERMARK_FILE);
        try (OutputStream fos = Files.newOutputStream(tmpFile);
                DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeByte(FORMAT_VERSION);
            dos.writeLong(snapshot.lsn.ledgerId);
            dos.writeLong(snapshot.lsn.offset);
            dos.writeInt(snapshot.numInstances);
            // Schema snapshot: tables
            dos.writeInt(snapshot.tables.size());
            for (Table t : snapshot.tables) {
                byte[] blob = t.serialize();
                dos.writeInt(blob.length);
                dos.write(blob);
            }
            // Schema snapshot: vector indexes
            dos.writeInt(snapshot.vectorIndexes.size());
            for (Index ix : snapshot.vectorIndexes) {
                byte[] blob = ix.serialize();
                dos.writeInt(blob.length);
                dos.write(blob);
            }
        }
        Files.move(tmpFile, watermarkFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        LOGGER.log(Level.FINE, "Saved watermark: {0}", snapshot);
    }
}
