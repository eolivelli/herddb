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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.utils.Bytes;
import herddb.utils.HerdDBByteBufAllocators;
import io.netty.buffer.ByteBuf;
import org.junit.Test;

/**
 * Verifies the {@link Record#release()} contract introduced in step 2 of
 * issue #399: idempotent release of off-heap-backed values, and a true no-op
 * on the existing on-heap-backed code path.
 */
public class RecordOffHeapTest {

    private Bytes newOffHeapValue(byte[] payload) {
        ByteBuf slice = HerdDBByteBufAllocators.dataPagesAllocator()
                .directBuffer(payload.length);
        slice.writeBytes(payload);
        return Bytes.fromOffHeap(slice);
    }

    @Test
    public void releaseFreesOffHeapValueAndIsIdempotent() {
        byte[] payload = "row-value".getBytes();
        Bytes value = newOffHeapValue(payload);
        Record r = new Record(Bytes.from_string("k1"), value);
        assertTrue("precondition: value is off-heap", value.isOffHeap());
        r.release();
        assertFalse("value slice released", value.isOffHeap());
        // Calling again must not throw.
        r.release();
        r.release();
    }

    @Test
    public void releaseIsNoOpOnOnHeapBackedRecord() {
        Record r = new Record(Bytes.from_string("k1"), Bytes.from_string("v1"));
        // Smoke: release on an on-heap record changes nothing observable.
        r.release();
        assertEquals("v1", r.value.to_string());
    }

    @Test
    public void offHeapValueRoundTripsForReadsAfterMaterialisation() {
        byte[] payload = "row-value".getBytes();
        Bytes value = newOffHeapValue(payload);
        Record r = new Record(Bytes.from_string("k2"), value);
        // Force a byte[] read; this materialises and releases the slice.
        assertEquals("row-value", r.value.to_string());
        assertFalse(r.value.isOffHeap());
        // After materialisation, release() is still safe.
        r.release();
    }

    @Test
    public void offHeapValueEqualsOnHeapValueWithSameBytes() {
        byte[] payload = "abc".getBytes();
        Bytes off = newOffHeapValue(payload);
        Bytes on = Bytes.from_array(payload);
        Record offRec = new Record(Bytes.from_string("k"), off);
        Record onRec = new Record(Bytes.from_string("k"), on);
        assertEquals(onRec, offRec);
        assertEquals(offRec, onRec);
        assertEquals(onRec.hashCode(), offRec.hashCode());
        offRec.release();
    }
}
