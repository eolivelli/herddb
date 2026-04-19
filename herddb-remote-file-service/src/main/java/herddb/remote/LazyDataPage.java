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

import herddb.core.DataPage;
import herddb.core.TableManager;
import herddb.model.Record;
import herddb.remote.LazyDataPageFormat.FixedHeader;
import herddb.remote.LazyDataPageFormat.RecordMetadata;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Immutable {@link DataPage} variant that holds only the primary keys and the
 * per-record value offsets of a v2 data page; the value bytes themselves are
 * fetched on demand from remote storage on each {@link #get(Bytes)} call,
 * going through the DSM's {@link LazyValueCache}.
 *
 * <p>Intended for read-heavy workloads against tables that do not fit in
 * heap, where a single query typically touches only a handful of records per
 * page. The heap cost of a resident {@code LazyDataPage} is proportional to
 * {@code numRecords × (keyLen + 12)} plus HashMap overhead — a fraction of
 * the eager cost when values are large.
 *
 * <p>Semantics:
 * <ul>
 *   <li>Always {@code immutable} — {@link #put} / {@link #remove} throw.
 *   <li>{@link #getRecordsForFlush()} throws — lazy pages are read-only and
 *       never participate in a checkpoint.
 *   <li>{@link #size()} / {@link #isEmpty()} / {@link #getKeysForDebug()}
 *       are served from the local key index and never trigger remote I/O.
 *   <li>{@link #get(Bytes)} returns {@code null} when the key is absent
 *       without issuing a remote call (the key check hits the local index).
 *   <li>Repeat {@code get(key)} calls for the same key go through the
 *       DSM-wide {@link LazyValueCache}; a hit avoids remote I/O entirely.
 * </ul>
 */
public final class LazyDataPage extends DataPage {

    /**
     * Overhead per in-memory index entry in bytes. Dominant terms:
     * <ul>
     *   <li>{@link Bytes} wrapper for the key: constant header + array ref + length int.
     *     Estimated per {@link Bytes#getEstimatedSize()}.
     *   <li>{@link RecordMetadata} wrapper: header + key ref + long (offset) + int (length).
     *     12 + 4 + 8 + 4 = 28 bytes (compressed oops); 16 + 8 + 8 + 4 + 4 = 40 bytes (no compressed oops).
     *   <li>HashMap$Node overhead: {@code DataPage.CONSTANT_ENTRY_BYTE_SIZE}.
     * </ul>
     * Value bytes themselves are NOT charged against the page's
     * {@code usedMemory} — they live in the shared, separately-bounded
     * {@link LazyValueCache}.
     */
    private static final long METADATA_OBJECT_OVERHEAD =
            herddb.utils.ObjectSizeUtils.COMPRESSED_OOPS ? 28L : 40L;

    private final RemoteFileDataStorageManager dsm;
    private final String tableSpace;
    private final String uuid;
    private final FixedHeader header;
    private final Map<Bytes, RecordMetadata> index;

    LazyDataPage(TableManager owner, long pageId, long maxSize,
                 RemoteFileDataStorageManager dsm, String tableSpace, String uuid,
                 FixedHeader header, Map<Bytes, RecordMetadata> index,
                 long estimatedUsedMemory) {
        super(owner, pageId, maxSize, estimatedUsedMemory, new HashMap<>(), true);
        this.dsm = dsm;
        this.tableSpace = tableSpace;
        this.uuid = uuid;
        this.header = header;
        this.index = index;
    }

    /**
     * Estimates the in-heap size of an index entry for memory accounting.
     * Exposed for tests.
     */
    public static long estimateIndexEntrySize(Bytes key) {
        return key.getEstimatedSize()
                + METADATA_OBJECT_OVERHEAD
                + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    /**
     * Builds a {@code LazyDataPage} from a parsed v2 header and index. The
     * returned page is immediately ready to serve {@link #get} lookups.
     */
    static LazyDataPage build(TableManager owner, long pageId, long maxSize,
            RemoteFileDataStorageManager dsm, String tableSpace, String uuid,
            FixedHeader header, java.util.List<RecordMetadata> metadata) {
        // Preserve iteration order for getKeysForDebug() to be predictable in tests.
        Map<Bytes, RecordMetadata> map = new LinkedHashMap<>(metadata.size() * 2);
        long estimated = 0L;
        for (RecordMetadata m : metadata) {
            map.put(m.key, m);
            estimated += estimateIndexEntrySize(m.key);
        }
        return new LazyDataPage(owner, pageId, maxSize, dsm, tableSpace, uuid,
                header, Collections.unmodifiableMap(map), estimated);
    }

    /** Number of records in this page (constant, no remote I/O). */
    public int indexSize() {
        return index.size();
    }

    @Override
    public Record get(Bytes key) {
        RecordMetadata m = index.get(key);
        if (m == null) {
            return null;
        }
        try {
            byte[] value = dsm.readPageValue(tableSpace, uuid, pageId,
                    header, m.valueOffset, m.valueLength);
            return new Record(key, Bytes.from_array(value));
        } catch (DataStorageManagerException e) {
            throw new LazyValueFetchException("Failed to fetch value for key "
                    + key + " from " + tableSpace + "/" + uuid + "#" + pageId, e);
        }
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public Set<Bytes> getKeysForDebug() {
        return index.keySet();
    }

    @Override
    public Collection<Record> getRecordsForFlush() {
        throw new UnsupportedOperationException(
                "LazyDataPage is read-only; a lazy page must never be flushed");
    }

    // LazyDataPage identity is inherited from DataPage (pageId-based).
    // Overriding equals/hashCode explicitly to silence SpotBugs'
    // EQ_DOESNT_OVERRIDE_EQUALS: this class adds no state that participates
    // in page equality.
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Wraps a remote fetch failure so the caller (typically
     * {@link herddb.core.TableManager}) sees a runtime exception instead of a
     * checked {@code DataStorageManagerException}. Callers can unwrap via
     * {@link Throwable#getCause()}.
     */
    public static final class LazyValueFetchException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        LazyValueFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
