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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Pins the JSON contract for {@link OptimizerTask}: round-trip equality,
 * unknown-property tolerance, and schema-version rejection for future writers.
 */
public class OptimizerTaskTest {

    private OptimizerTask sampleTask() {
        Map<String, Integer> versions = new LinkedHashMap<>();
        versions.put("seg-a", 4);
        versions.put("seg-b", 7);
        return OptimizerTask.builder()
                .taskId("task-1")
                .tablespaceUuid("ts-uuid")
                .indexUuid("idx-uuid")
                .inputSegmentUuids(List.of("seg-a", "seg-b"))
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(2)
                .state(OptimizerTaskState.PENDING)
                .attempts(0)
                .createdAtEpochMillis(1_700_000_000_000L)
                .leaderEpoch(42L)
                .build();
    }

    @Test
    public void roundTripEqualsOriginal() throws Exception {
        OptimizerTask original = sampleTask();
        OptimizerTask round = OptimizerTask.deserialize(original.serialize());
        assertEquals(original, round);
        assertEquals(original.hashCode(), round.hashCode());
    }

    @Test
    public void roundTripWithClaimAndLastError() throws Exception {
        OptimizerTask original = sampleTask().toBuilder()
                .state(OptimizerTaskState.CLAIMED)
                .claim(new OptimizerTask.WorkerClaim("worker-uuid", 1_700_000_001_000L))
                .attempts(1)
                .lastError(new OptimizerTask.LastError("worker-uuid", "transient", 1_700_000_000_500L))
                .build();
        OptimizerTask round = OptimizerTask.deserialize(original.serialize());
        assertEquals(original, round);
    }

    @Test
    public void unknownPropertyIsIgnoredOnRead() throws Exception {
        String json = "{\"schemaVersion\":1,\"taskId\":\"t\",\"tablespaceUuid\":\"ts\",\"indexUuid\":\"ix\","
                + "\"state\":\"PENDING\",\"attempts\":0,\"createdAtEpochMillis\":0,\"leaderEpoch\":1,"
                + "\"targetOwnerInstanceId\":0,\"unknownNewField\":\"future-extension\"}";
        OptimizerTask t = OptimizerTask.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals("t", t.getTaskId());
        assertEquals(OptimizerTaskState.PENDING, t.getState());
    }

    @Test
    public void missingSchemaVersionDefaultsToOne() throws Exception {
        String json = "{\"taskId\":\"t\",\"tablespaceUuid\":\"ts\",\"indexUuid\":\"ix\","
                + "\"state\":\"PENDING\",\"attempts\":0,\"createdAtEpochMillis\":0,\"leaderEpoch\":0,"
                + "\"targetOwnerInstanceId\":0}";
        OptimizerTask t = OptimizerTask.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(OptimizerTask.SCHEMA_VERSION, t.getSchemaVersion());
    }

    @Test
    public void rejectsSchemaVersionAboveMaxSupported() {
        String json = "{\"schemaVersion\":9999,\"taskId\":\"t\",\"tablespaceUuid\":\"ts\",\"indexUuid\":\"ix\","
                + "\"state\":\"PENDING\",\"attempts\":0,\"createdAtEpochMillis\":0,\"leaderEpoch\":0,"
                + "\"targetOwnerInstanceId\":0}";
        try {
            OptimizerTask.deserialize(json.getBytes(StandardCharsets.UTF_8));
            fail("expected IOException for unsupported schemaVersion");
        } catch (IOException ok) {
            assertTrue(ok.getMessage().contains("schemaVersion"));
        }
    }

    @Test
    public void toBuilderPreservesAllFields() {
        OptimizerTask original = sampleTask();
        OptimizerTask copy = original.toBuilder().build();
        assertEquals(original, copy);
    }

    @Test
    public void inputSegmentUuidsListIsImmutable() {
        OptimizerTask t = sampleTask();
        try {
            t.getInputSegmentUuids().add("seg-c");
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ok) {
            // expected
        }
    }

    @Test
    public void inputSegmentExpectedVersionsMapIsImmutable() {
        OptimizerTask t = sampleTask();
        try {
            t.getInputSegmentExpectedVersions().put("seg-c", 1);
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ok) {
            // expected
        }
    }

    @Test
    public void claimAndLastErrorAreNullableByDefault() {
        OptimizerTask t = sampleTask();
        assertNull(t.getClaim());
        assertNull(t.getLastError());
        assertNotNull(t.toString());
    }

    @Test
    public void schemaVersionFieldIsOneByDefault() {
        assertEquals(1, sampleTask().getSchemaVersion());
    }

    @Test
    public void missingStateFieldDefaultsToPending() throws Exception {
        // Forward-rolled writer or buggy producer may emit a JSON without
        // "state" — the @JsonCreator must default to PENDING rather than NPE.
        String json = "{\"taskId\":\"t\",\"tablespaceUuid\":\"ts\",\"indexUuid\":\"ix\","
                + "\"attempts\":0,\"createdAtEpochMillis\":0,\"leaderEpoch\":0,"
                + "\"targetOwnerInstanceId\":0}";
        OptimizerTask t = OptimizerTask.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(OptimizerTaskState.PENDING, t.getState());
    }
}
