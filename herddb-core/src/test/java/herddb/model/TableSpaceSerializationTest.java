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

package herddb.model;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Round-trip tests for {@link TableSpace} serialisation.
 *
 * @author enrico.olivelli
 */
public class TableSpaceSerializationTest {

    @Test
    public void roundTrip() throws Exception {
        TableSpace ts = TableSpace.builder()
                .uuid("abc123")
                .name("default")
                .leader("node1")
                .replica("node1")
                .replica("node2")
                .expectedReplicaCount(2)
                .build();
        TableSpace restored = TableSpace.deserialize(ts.serialize(), null, 0);
        assertEquals(ts.uuid, restored.uuid);
        assertEquals(ts.name, restored.name);
        assertEquals(ts.leaderId, restored.leaderId);
        assertEquals(ts.replicas, restored.replicas);
        assertEquals(ts.expectedReplicaCount, restored.expectedReplicaCount);
        assertEquals(ts.maxLeaderInactivityTime, restored.maxLeaderInactivityTime);
    }

    @Test
    public void cloningPreservesFields() throws Exception {
        TableSpace src = TableSpace.builder()
                .uuid("a")
                .name("default")
                .leader("n1")
                .replica("n1")
                .expectedReplicaCount(1)
                .maxLeaderInactivityTime(60_000L)
                .build();
        TableSpace clone = TableSpace.builder().cloning(src).build();
        assertEquals(src.uuid, clone.uuid);
        assertEquals(src.expectedReplicaCount, clone.expectedReplicaCount);
        assertEquals(src.maxLeaderInactivityTime, clone.maxLeaderInactivityTime);
    }
}
