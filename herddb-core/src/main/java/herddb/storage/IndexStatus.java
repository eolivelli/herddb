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

package herddb.storage;

import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.MetadataCompression;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Status of an index on disk
 *
 * @author enrico.olivelli
 */
public class IndexStatus {

    public final String indexName;
    public final LogSequenceNumber sequenceNumber;
    public final byte[] indexData;
    public final Set<Long> activePages;
    public final long newPageId;

    public IndexStatus(
            String indexName, LogSequenceNumber sequenceNumber,
            long newPageId, final Set<Long> activePages, byte[] indexData
    ) {
        this.indexName = indexName;
        this.sequenceNumber = sequenceNumber;
        this.newPageId = newPageId;
        this.indexData = indexData != null ? indexData : new byte[0];
        this.activePages = activePages != null ? activePages : Collections.emptySet();
    }


    /**
     * {@code flags} bit 0: the remaining payload (everything after the flags
     * field) is written as a single length-prefixed GZIP-compressed byte
     * array. Keeps per-checkpoint on-disk / znode size bounded even when
     * {@link #activePages} grows to thousands of entries.
     */
    static final long FLAG_COMPRESSED = 0x1L;

    public void serialize(ExtendedDataOutputStream output) throws IOException {
        output.writeVLong(1); // version
        output.writeVLong(FLAG_COMPRESSED); // flags

        VisibleByteArrayOutputStream payload = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream inner = new ExtendedDataOutputStream(payload)) {
            writePayload(inner);
        }
        output.writeArray(MetadataCompression.compressGzip(payload.toByteArray()));
    }

    private void writePayload(ExtendedDataOutputStream output) throws IOException {
        output.writeUTF(indexName);
        output.writeLong(sequenceNumber.ledgerId);
        output.writeLong(sequenceNumber.offset);
        output.writeVLong(newPageId);
        output.writeVInt(activePages.size());
        for (long idpage : activePages) {
            output.writeVLong(idpage);
        }
        output.writeArray(indexData);
    }

    public static IndexStatus deserialize(ExtendedDataInputStream in) throws IOException {
        long version = in.readVLong(); // version
        long flags = in.readVLong(); // flags
        if (version != 1) {
            throw new DataStorageManagerException("corrupted index status (version " + version + ")");
        }
        if ((flags & ~FLAG_COMPRESSED) != 0) {
            throw new DataStorageManagerException("corrupted index status (unknown flags " + flags + ")");
        }
        if ((flags & FLAG_COMPRESSED) != 0) {
            byte[] compressed = in.readArray();
            byte[] raw = MetadataCompression.decompressGzip(compressed);
            try (ExtendedDataInputStream inner = new ExtendedDataInputStream(new SimpleByteArrayInputStream(raw))) {
                return readPayload(inner);
            }
        }
        // Legacy path: plain payload follows the flags field directly.
        return readPayload(in);
    }

    private static IndexStatus readPayload(ExtendedDataInputStream in) throws IOException {
        String indexName = in.readUTF();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        long nextPageId = in.readVLong();
        int numPages = in.readVInt();
        Set<Long> activePages = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            activePages.add(in.readVLong());
        }
        byte[] indexData = in.readArray();
        return new IndexStatus(indexName, new LogSequenceNumber(ledgerId, offset), nextPageId, activePages, indexData);
    }

    @Override
    public String toString() {
        return "IndexStatus{" + "indexName=" + indexName + ", sequenceNumber=" + sequenceNumber + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 13 * hash + Objects.hashCode(indexName);
        hash = 13 * hash + Objects.hashCode(sequenceNumber);
        hash = 13 * hash + Objects.hashCode(newPageId);
        hash = 13 * hash + Arrays.hashCode(indexData);
        hash = 13 * hash + Objects.hashCode(activePages);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IndexStatus other = (IndexStatus) obj;
        if (!Objects.equals(this.indexName, other.indexName)) {
            return false;
        }
        if (!Objects.equals(this.sequenceNumber, other.sequenceNumber)) {
            return false;
        }
        if (!Objects.equals(this.newPageId, other.newPageId)) {
            return false;
        }
        if (!Arrays.equals(this.indexData, other.indexData)) {
            return false;
        }
        return Objects.equals(this.activePages, other.activePages);
    }

}
