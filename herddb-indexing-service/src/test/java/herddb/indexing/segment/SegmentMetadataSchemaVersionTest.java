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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Validates fix D5: the {@code schemaVersion} field on {@link SegmentMetadata}.
 *
 * <ul>
 *   <li>The serializer always emits the current {@link SegmentMetadata#SCHEMA_VERSION}.</li>
 *   <li>Payloads without a {@code schemaVersion} field deserialize as v1 (legacy support).</li>
 *   <li>A future-version payload (schemaVersion > MAX_SUPPORTED) is rejected loudly.</li>
 * </ul>
 */
public class SegmentMetadataSchemaVersionTest {

    private static SegmentMetadata sample() {
        return SegmentMetadata.builder()
                .segmentUuid("seg-X")
                .tablespaceUuid("ts").tableName("docs")
                .indexUuid("idx").indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(10L).generation(1L)
                .createdAtEpochMillis(0L).build();
    }

    @Test
    public void serializerStampsCurrentSchemaVersion() {
        SegmentMetadata m = sample();
        assertEquals("builder must stamp current schemaVersion",
                SegmentMetadata.SCHEMA_VERSION, m.getSchemaVersion());
        String json = new String(m.serialize(), StandardCharsets.UTF_8);
        assertTrue("serialized JSON must include schemaVersion",
                json.contains("\"schemaVersion\":" + SegmentMetadata.SCHEMA_VERSION));
    }

    @Test
    public void payloadWithoutSchemaVersionDeserializesAsV1() throws Exception {
        // Hand-craft a JSON without the schemaVersion field — simulating a payload
        // written by an earlier code revision before the field existed.
        String json = "{"
                + "\"segmentUuid\":\"seg-X\","
                + "\"tablespaceUuid\":\"ts\","
                + "\"tableName\":\"docs\","
                + "\"indexUuid\":\"idx\","
                + "\"indexName\":\"docs_v1\","
                + "\"state\":\"ACTIVE\","
                + "\"ownerInstanceId\":0,"
                + "\"pendingOwnerInstanceId\":-1,"
                + "\"sizeBytes\":100,"
                + "\"vectorCount\":10,"
                + "\"generation\":1,"
                + "\"baseLsnLedgerId\":1,"
                + "\"baseLsnOffset\":100"
                + "}";
        SegmentMetadata m = SegmentMetadata.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals("missing schemaVersion must default to 1", 1, m.getSchemaVersion());
        assertEquals("seg-X", m.getSegmentUuid());
    }

    @Test
    public void futureSchemaVersionIsRejected() {
        String json = "{"
                + "\"schemaVersion\":99,"
                + "\"segmentUuid\":\"seg-X\","
                + "\"tablespaceUuid\":\"ts\","
                + "\"tableName\":\"docs\","
                + "\"indexUuid\":\"idx\","
                + "\"indexName\":\"docs_v1\","
                + "\"state\":\"ACTIVE\","
                + "\"ownerInstanceId\":0,"
                + "\"pendingOwnerInstanceId\":-1,"
                + "\"sizeBytes\":100,"
                + "\"vectorCount\":10,"
                + "\"generation\":1"
                + "}";
        try {
            SegmentMetadata.deserialize(json.getBytes(StandardCharsets.UTF_8));
            fail("expected IOException for future schemaVersion");
        } catch (IOException ok) {
            assertTrue("error must mention schemaVersion: " + ok.getMessage(),
                    ok.getMessage().contains("schemaVersion"));
            assertTrue("error must include the offending number",
                    ok.getMessage().contains("99"));
        }
    }

    @Test
    public void roundTripPreservesSchemaVersion() throws Exception {
        SegmentMetadata original = sample();
        SegmentMetadata roundtrip = SegmentMetadata.deserialize(original.serialize());
        assertEquals(original.getSchemaVersion(), roundtrip.getSchemaVersion());
        assertEquals(original.getSegmentUuid(), roundtrip.getSegmentUuid());
    }
}
