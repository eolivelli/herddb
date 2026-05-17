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
import static org.junit.jupiter.api.Assertions.assertThrows;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
}
