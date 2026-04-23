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
                new IndexingServiceCheckpointState(2, 10L, 100L, 5, 1_700_000_000_000L);
        byte[] bytes = original.serialize();
        IndexingServiceCheckpointState decoded =
                IndexingServiceCheckpointState.deserialize(bytes);
        assertEquals(original, decoded);
    }
}
