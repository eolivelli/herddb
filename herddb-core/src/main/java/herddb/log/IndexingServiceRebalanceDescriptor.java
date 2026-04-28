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

package herddb.log;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Payload of an {@link LogEntryType#INDEXING_SERVICE_REBALANCE} log entry.
 *
 * <p>Carries the new effective {@code numInstances} for every indexing-service
 * replica along with a snapshot of every {@link Table} and vector {@link Index}
 * present on the leader at write time. The schema snapshot is intended for
 * indexing-service replicas that boot when the BookKeeper history has been
 * trimmed and cannot replay {@code CREATE_TABLE} / {@code CREATE_INDEX}
 * entries from {@code START_OF_TIME}.
 *
 * <p>Each {@link Index} blob already contains its per-index
 * {@code numInstances} stamped at CREATE INDEX time, so an indexing-service
 * replica that receives this descriptor has everything it needs to start
 * accepting routed mutations.
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public final class IndexingServiceRebalanceDescriptor {

    public static final byte SCHEMA_VERSION = 1;

    public final long epoch;
    public final int defaultNumInstances;
    public final List<Table> tables;
    public final List<Index> vectorIndexes;

    public IndexingServiceRebalanceDescriptor(long epoch, int defaultNumInstances,
                                               List<Table> tables, List<Index> vectorIndexes) {
        if (defaultNumInstances < 1) {
            throw new IllegalArgumentException("defaultNumInstances must be >= 1");
        }
        this.epoch = epoch;
        this.defaultNumInstances = defaultNumInstances;
        this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
        this.vectorIndexes = Collections.unmodifiableList(new ArrayList<>(vectorIndexes));
    }

    public byte[] serialize() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ExtendedDataOutputStream out = new ExtendedDataOutputStream(bos)) {
            out.writeByte(SCHEMA_VERSION);
            out.writeLong(epoch);
            out.writeVInt(defaultNumInstances);
            out.writeVInt(tables.size());
            for (Table t : tables) {
                byte[] blob = t.serialize();
                out.writeVInt(blob.length);
                out.write(blob);
            }
            out.writeVInt(vectorIndexes.size());
            for (Index ix : vectorIndexes) {
                byte[] blob = ix.serialize();
                out.writeVInt(blob.length);
                out.write(blob);
            }
            out.flush();
            return bos.toByteArray();
        } catch (IOException impossible) {
            // Backed by ByteArrayOutputStream, cannot throw.
            throw new IllegalStateException(impossible);
        }
    }

    public static IndexingServiceRebalanceDescriptor deserialize(byte[] data) throws IOException {
        try (SimpleByteArrayInputStream bin = new SimpleByteArrayInputStream(data);
                ExtendedDataInputStream in = new ExtendedDataInputStream(bin)) {
            byte version = in.readByte();
            if (version != SCHEMA_VERSION) {
                throw new IOException("unsupported INDEXING_SERVICE_REBALANCE schema version " + version);
            }
            long epoch = in.readLong();
            int defaultNumInstances = in.readVInt();
            int numTables = in.readVInt();
            List<Table> tables = new ArrayList<>(numTables);
            for (int i = 0; i < numTables; i++) {
                int blobLen = in.readVInt();
                byte[] blob = new byte[blobLen];
                in.readFully(blob);
                tables.add(Table.deserialize(blob));
            }
            int numIndexes = in.readVInt();
            List<Index> indexes = new ArrayList<>(numIndexes);
            for (int i = 0; i < numIndexes; i++) {
                int blobLen = in.readVInt();
                byte[] blob = new byte[blobLen];
                in.readFully(blob);
                indexes.add(Index.deserialize(blob));
            }
            return new IndexingServiceRebalanceDescriptor(epoch, defaultNumInstances, tables, indexes);
        }
    }
}
