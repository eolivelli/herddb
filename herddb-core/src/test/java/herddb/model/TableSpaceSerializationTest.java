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
import static org.junit.Assert.assertThrows;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/**
 * Round-trip tests for {@link TableSpace} serialisation, including
 * backward-compatibility for v1 payloads written before
 * {@link TableSpace#defaultIndexingNumInstances} existed.
 *
 * @author enrico.olivelli
 */
public class TableSpaceSerializationTest {

    @Test
    public void roundTripDefault() throws Exception {
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
        assertEquals(TableSpace.DEFAULT_INDEXING_NUM_INSTANCES_DEFAULT,
                restored.defaultIndexingNumInstances);
    }

    @Test
    public void roundTripExplicitNumInstances() throws Exception {
        TableSpace ts = TableSpace.builder()
                .uuid("xyz789")
                .name("default")
                .leader("node1")
                .replica("node1")
                .defaultIndexingNumInstances(8)
                .build();
        TableSpace restored = TableSpace.deserialize(ts.serialize(), null, 0);
        assertEquals(8, restored.defaultIndexingNumInstances);
    }

    /**
     * Backward compatibility: a v1 payload (no defaultIndexingNumInstances
     * field) deserialises with the field defaulted. Lets a post-feature
     * leader read tablespace metadata that an old node persisted before the
     * upgrade.
     */
    @Test
    public void deserialiseLegacyV1Payload() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(bos)) {
            out.writeVLong(1); // version 1
            out.writeVLong(0); // flags
            out.writeUTF("legacy-uuid");
            out.writeUTF("legacy-name");
            out.writeUTF("leader-1");
            out.writeVInt(1); // expectedReplicaCount
            out.writeVInt(1); // numreplicas
            out.writeUTF("leader-1");
            out.writeVLong(0L); // maxLeaderInactivityTime
            // no defaultIndexingNumInstances in v1
        }
        TableSpace restored = TableSpace.deserialize(bos.toByteArray(), null, 0);
        assertEquals("legacy-uuid", restored.uuid);
        assertEquals("legacy-name", restored.name);
        assertEquals(TableSpace.DEFAULT_INDEXING_NUM_INSTANCES_DEFAULT,
                restored.defaultIndexingNumInstances);
    }

    @Test
    public void cloningPreservesNumInstances() throws Exception {
        TableSpace src = TableSpace.builder()
                .uuid("a")
                .name("default")
                .leader("n1")
                .replica("n1")
                .defaultIndexingNumInstances(4)
                .build();
        TableSpace clone = TableSpace.builder().cloning(src).build();
        assertEquals(4, clone.defaultIndexingNumInstances);
    }

    @Test
    public void unknownVersionRejected() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(bos)) {
            out.writeVLong(3); // unknown future version
            out.writeVLong(0);
        }
        IOException ex = assertThrows(IOException.class,
                () -> TableSpace.deserialize(new ExtendedDataInputStream(
                        new SimpleByteArrayInputStream(bos.toByteArray())), null, 0));
        assertEquals("corrupted tablespace file", ex.getMessage());
    }

    @Test
    public void zeroNumInstancesRejectedAtBuild() {
        assertThrows(IllegalArgumentException.class,
                () -> TableSpace.builder()
                        .uuid("u")
                        .name("default")
                        .leader("n1")
                        .replica("n1")
                        .defaultIndexingNumInstances(0)
                        .build());
    }
}
