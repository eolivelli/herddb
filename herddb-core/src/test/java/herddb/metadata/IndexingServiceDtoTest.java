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

package herddb.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class IndexingServiceDtoTest {

    @Test
    public void primaryRoundTrip() throws Exception {
        IndexingServiceInstanceDescriptor original =
                IndexingServiceInstanceDescriptor.primary("p0", "host:9850", 3);
        byte[] bytes = original.serialize();
        IndexingServiceInstanceDescriptor decoded =
                IndexingServiceInstanceDescriptor.deserialize(bytes);
        assertEquals(original, decoded);
        assertEquals("primary", decoded.getRole());
        assertEquals(3, decoded.getInstanceId());
        assertEquals(IndexingServiceInstanceDescriptor.NO_SHADOW_OF, decoded.getShadowOf());
    }

    @Test
    public void shadowRoundTrip() throws Exception {
        IndexingServiceInstanceDescriptor original =
                IndexingServiceInstanceDescriptor.shadow("s0a", "host-s0a:9850", 0);
        byte[] bytes = original.serialize();
        IndexingServiceInstanceDescriptor decoded =
                IndexingServiceInstanceDescriptor.deserialize(bytes);
        assertEquals(original, decoded);
        assertTrue(decoded.isShadow());
        assertEquals(0, decoded.getShadowOf());
        assertEquals(0, decoded.effectiveInstanceId());
    }

    @Test
    public void checkpointStateRoundTrip() throws Exception {
        IndexingServiceCheckpointState original =
                new IndexingServiceCheckpointState(
                        2, 10L, 100L, 5, 1_700_000_000_000L, 1_699_999_999_500L);
        byte[] bytes = original.serialize();
        IndexingServiceCheckpointState decoded =
                IndexingServiceCheckpointState.deserialize(bytes);
        assertEquals(original, decoded);
        assertEquals(1_699_999_999_500L, decoded.getLastEntryTimestampMillis());
    }

    /**
     * Issue #423: a primary running new code may publish state that an old
     * shadow doesn't know how to consume — but Jackson's
     * {@code @JsonIgnoreProperties(ignoreUnknown=true)} is supposed to make
     * the reverse direction safe (old primary → new shadow), with the new
     * field defaulting to {@code 0} ("unknown"). Verify that here.
     */
    @Test
    public void checkpointStateMissingTimestampDefaultsToZero() throws Exception {
        // Hand-craft a JSON document that omits lastEntryTimestampMillis.
        byte[] jsonWithoutNewField =
                ("{\"instanceId\":2,\"ledgerId\":10,\"offset\":100,\"segmentCount\":5,"
                        + "\"timestampMillis\":1700000000000}")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        IndexingServiceCheckpointState decoded =
                IndexingServiceCheckpointState.deserialize(jsonWithoutNewField);
        assertEquals(2, decoded.getInstanceId());
        assertEquals(10L, decoded.getLedgerId());
        assertEquals(100L, decoded.getOffset());
        assertEquals(5, decoded.getSegmentCount());
        assertEquals(1_700_000_000_000L, decoded.getTimestampMillis());
        assertEquals("missing field defaults to 0 (unknown)",
                0L, decoded.getLastEntryTimestampMillis());
    }
}
