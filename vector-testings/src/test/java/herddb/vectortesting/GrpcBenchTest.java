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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link GrpcBench} internals. */
class GrpcBenchTest {

    @Test
    void pushBatchReleasesBuffersWhenSerializationFailsMidBatch() {
        // A serializer that hands out two pooled direct buffers, then throws on
        // the third entry — simulating LogEntry.serializeAsByteBuf() failing
        // mid-batch. pushBatch must release the two already-allocated buffers
        // rather than leak them.
        List<ByteBuf> handed = new ArrayList<>();
        Function<LogEntry, ByteBuf> serializer = entry -> {
            if (handed.size() == 2) {
                throw new IllegalStateException("simulated serialization failure");
            }
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(16);
            handed.add(buf);
            return buf;
        };
        List<LogEntry> entries = Arrays.asList(
                LogEntryFactory.noop(), LogEntryFactory.noop(),
                LogEntryFactory.noop(), LogEntryFactory.noop());

        // The client is never reached: serialization fails before pushEntries().
        assertThrows(IllegalStateException.class, () ->
                GrpcBench.pushBatch(null, 1L, new long[]{1L}, entries, serializer));

        assertEquals(2, handed.size(), "the serializer should have produced two buffers");
        for (ByteBuf buf : handed) {
            assertEquals(0, buf.refCnt(),
                    "every allocated buffer must be released after a mid-batch failure");
        }
    }

    /**
     * Issue #632: the admin {@code /status} endpoint must reflect the bench's
     * current phase rather than always reporting {@code idle}. The default
     * supplier installed on a fresh {@link BenchRuntime} returns
     * {@code phase=idle} — verify that {@link BenchRuntime#setStatusSupplier}
     * does swap it out for the supplier {@code GrpcBench} would install during
     * ingestion, and that the supplier carries the documented numeric fields.
     */
    @Test
    void benchRuntimeStatusSupplierIsSwappable() {
        Config config = new Config();
        BenchRuntime runtime = new BenchRuntime(config);

        // Default — what /status used to permanently report for grpc mode.
        Map<String, Object> initial = runtime.getStatusSupplier().get();
        assertEquals("idle", initial.get("phase"));

        runtime.setStatusSupplier(() -> Map.of(
                "phase", "ingest",
                "rows", 42L,
                "total", 100L,
                "ops_per_sec", 13.5,
                "push_calls", 7L));

        Map<String, Object> updated = runtime.getStatusSupplier().get();
        assertEquals("ingest", updated.get("phase"));
        assertEquals(42L, updated.get("rows"));
        assertEquals(100L, updated.get("total"));
        assertEquals(7L, updated.get("push_calls"));
        assertNotNull(updated.get("ops_per_sec"));
    }

    /**
     * Issue #632: in push mode the verification phase must <em>not</em> wait
     * on {@code --wait-for-indexes-timeout} (a JDBC-only flag) and must not
     * inherit the previous hard-coded 1-hour deadline. It returns immediately
     * the moment the indexed count reaches the expected value.
     */
    @Test
    void verifyVectorCountReturnsImmediatelyOnHit() throws Exception {
        Config config = new Config();
        BenchOutput out = BenchOutput.create(config);
        long start = System.nanoTime();
        // Counter already at the expected value — happy path: zero waits.
        GrpcBench.verifyVectorCount(() -> 100L, out, 100L, null, 60_000L, 50L);
        double elapsedMs = (System.nanoTime() - start) / 1e6;
        assertTrue(elapsedMs < 1_000.0,
                "verification must return immediately when the index is already up to date,"
                        + " elapsed=" + elapsedMs + " ms");
    }

    /**
     * Issue #632: when the indexing service never catches up, verification
     * must fail within the (short) push-mode timeout — proving the new bound
     * replaces the old 1-hour wait and is not driven by the JDBC
     * {@code --wait-for-indexes-timeout} flag. We pass a 2-second cap to keep
     * the test fast; the production constant is 30 s.
     */
    @Test
    void verifyVectorCountFailsFastWhenIndexFallsShort() {
        Config config = new Config();
        // Deliberately set the JDBC flag to something huge to prove it is not consulted.
        config.waitForIndexesTimeoutSeconds = 86_400;
        BenchOutput out = BenchOutput.create(config);
        long start = System.nanoTime();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                GrpcBench.verifyVectorCount(() -> 5L, out, 100L, null, 2_000L, 50L));
        double elapsedMs = (System.nanoTime() - start) / 1e6;
        assertTrue(elapsedMs >= 1_900.0 && elapsedMs < 4_000.0,
                "verification must fail close to the provided timeoutMs (2000), elapsed=" + elapsedMs + " ms");
        assertTrue(ex.getMessage().contains("reached only 5"), "expected message to report the last observed count, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("within 2000 ms"), "expected message to report the bound, got: " + ex.getMessage());
    }

    /**
     * Issue #632: the verification phase must update the admin status supplier
     * to {@code phase=verification} with {@code vector_count} and
     * {@code expected} fields — so the supervision loop can observe the
     * phase even while waiting for the tailer to apply the last batch.
     */
    @Test
    void verifyVectorCountPublishesPhaseToBenchRuntime() throws Exception {
        Config config = new Config();
        BenchRuntime runtime = new BenchRuntime(config);
        BenchOutput out = BenchOutput.create(config);
        // Counter at expected → returns immediately, but must have installed
        // the verification supplier before returning. We capture the supplier
        // by inspecting the runtime state.
        GrpcBench.verifyVectorCount(() -> 50L, out, 50L, runtime, 60_000L, 50L);
        Map<String, Object> status = runtime.getStatusSupplier().get();
        assertEquals("verification", status.get("phase"));
        assertEquals(50L, status.get("vector_count"));
        assertEquals(50L, status.get("expected"));
    }
}
