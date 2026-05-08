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
package herddb.indexing.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class SegmentMetadataTest {

    @Test
    public void roundTripActiveSegment() throws Exception {
        SegmentMetadata original = SegmentMetadata.builder()
                .segmentUuid("seg-1")
                .tablespaceUuid("ts-1")
                .tableName("docs")
                .indexUuid("idx-1")
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .graphPath("ts-1/idx-1_seg-1/graph")
                .mapPath("ts-1/idx-1_seg-1/map")
                .baseLsn(new LogSequenceNumber(7L, 42L))
                .sizeBytes(1234567L)
                .vectorCount(1000L)
                .generation(1L)
                .createdAtEpochMillis(1_700_000_000L)
                .build();

        byte[] data = original.serialize();
        SegmentMetadata roundtripped = SegmentMetadata.deserialize(data);

        assertEquals(original, roundtripped);
        assertEquals("seg-1", roundtripped.getSegmentUuid());
        assertEquals(SegmentState.ACTIVE, roundtripped.getState());
        assertEquals(0, roundtripped.getOwnerInstanceId());
        assertEquals(SegmentMetadata.NO_INSTANCE, roundtripped.getPendingOwnerInstanceId());
        assertEquals(7L, roundtripped.baseLsn().ledgerId);
        assertEquals(42L, roundtripped.baseLsn().offset);
        assertNull(roundtripped.tombstoneLsn());
        assertEquals(Collections.emptyList(), roundtripped.getReplacedBy());
    }

    @Test
    public void roundTripDeprecatedSegmentWithReplacedBy() throws Exception {
        SegmentMetadata original = SegmentMetadata.builder()
                .segmentUuid("seg-old")
                .tablespaceUuid("ts-1")
                .tableName("docs")
                .indexUuid("idx-1")
                .indexName("docs_v1")
                .state(SegmentState.DEPRECATED)
                .ownerInstanceId(2)
                .replacedBy(Arrays.asList("seg-new-a", "seg-new-b"))
                .retentionUntilEpochMillis(1_800_000_000L)
                .createdAtEpochMillis(1_700_000_000L)
                .build();

        SegmentMetadata roundtripped = SegmentMetadata.deserialize(original.serialize());

        assertEquals(SegmentState.DEPRECATED, roundtripped.getState());
        assertEquals(Arrays.asList("seg-new-a", "seg-new-b"), roundtripped.getReplacedBy());
        assertEquals(1_800_000_000L, roundtripped.getRetentionUntilEpochMillis());
    }

    @Test
    public void builderClonesViaToBuilder() {
        SegmentMetadata original = SegmentMetadata.builder()
                .segmentUuid("seg-1")
                .tablespaceUuid("ts-1")
                .tableName("docs")
                .indexUuid("idx-1")
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .build();

        SegmentMetadata transferring = original.toBuilder()
                .state(SegmentState.TRANSFERRING)
                .pendingOwnerInstanceId(1)
                .build();

        assertEquals(SegmentState.TRANSFERRING, transferring.getState());
        assertEquals(0, transferring.getOwnerInstanceId());
        assertEquals(1, transferring.getPendingOwnerInstanceId());
        // The original is unchanged.
        assertEquals(SegmentState.ACTIVE, original.getState());
        assertEquals(SegmentMetadata.NO_INSTANCE, original.getPendingOwnerInstanceId());
    }

    @Test
    public void toBuilderWithTombstoneLsn() {
        SegmentMetadata withTombstone = SegmentMetadata.builder()
                .segmentUuid("seg-1")
                .tablespaceUuid("ts-1")
                .tableName("docs")
                .indexUuid("idx-1")
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .tombstoneLsn(new LogSequenceNumber(11L, 22L))
                .tombstonePath("ts-1/idx-1_seg-1/tombstones-1")
                .build();

        assertEquals(11L, withTombstone.tombstoneLsn().ledgerId);
        assertEquals(22L, withTombstone.tombstoneLsn().offset);
        assertEquals("ts-1/idx-1_seg-1/tombstones-1", withTombstone.getTombstonePath());

        SegmentMetadata cleared = withTombstone.toBuilder().tombstoneLsn(null).tombstonePath(null).build();
        assertNull(cleared.tombstoneLsn());
        assertNull(cleared.getTombstonePath());
    }

    @Test
    public void deserializerIgnoresUnknownFields() throws Exception {
        // future-compat: deserializer should accept a payload with extra fields it
        // doesn't know about, since SegmentMetadata uses @JsonIgnoreProperties(ignoreUnknown = true).
        String json = "{\"segmentUuid\":\"seg-1\",\"tablespaceUuid\":\"ts-1\",\"tableName\":\"docs\","
                + "\"indexUuid\":\"idx-1\",\"indexName\":\"docs_v1\",\"state\":\"ACTIVE\","
                + "\"ownerInstanceId\":0,\"pendingOwnerInstanceId\":-1,\"someFutureField\":42}";
        SegmentMetadata m = SegmentMetadata.deserialize(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertNotNull(m);
        assertEquals("seg-1", m.getSegmentUuid());
        assertEquals(SegmentState.ACTIVE, m.getState());
    }

    @Test
    public void equalityIsValueBased() {
        SegmentMetadata a = SegmentMetadata.builder()
                .segmentUuid("seg-1").tablespaceUuid("ts-1").tableName("docs")
                .indexUuid("idx-1").indexName("docs_v1").state(SegmentState.ACTIVE).build();
        SegmentMetadata b = SegmentMetadata.builder()
                .segmentUuid("seg-1").tablespaceUuid("ts-1").tableName("docs")
                .indexUuid("idx-1").indexName("docs_v1").state(SegmentState.ACTIVE).build();
        SegmentMetadata c = a.toBuilder().generation(99L).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(!a.equals(c));
    }
}
