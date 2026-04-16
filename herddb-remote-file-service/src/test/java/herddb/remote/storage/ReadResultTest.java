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

package herddb.remote.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

public class ReadResultTest {

    @Test
    public void testFound() {
        byte[] data = {1, 2, 3};
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(data.length);
        buf.writeBytes(data);
        ReadResult result = ReadResult.found(buf);
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testNotFound() {
        ReadResult result = ReadResult.notFound();
        assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        assertNull(result.content());
        result.release(); // Safe to call on notFound
    }
}
