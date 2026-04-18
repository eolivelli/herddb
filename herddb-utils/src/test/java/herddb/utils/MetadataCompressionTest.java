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

package herddb.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class MetadataCompressionTest {

    @Test
    public void looksGzipRecognisesMagic() {
        assertTrue(MetadataCompression.looksGzip(new byte[]{(byte) 0x1f, (byte) 0x8b, 0, 0}));
    }

    @Test
    public void looksGzipRejectsPlainText() {
        assertFalse(MetadataCompression.looksGzip("{\"activeLedgers\":[]}".getBytes(StandardCharsets.UTF_8)));
        assertFalse(MetadataCompression.looksGzip(new byte[]{1, 2}));
        assertFalse(MetadataCompression.looksGzip(new byte[]{}));
        assertFalse(MetadataCompression.looksGzip(null));
    }

    @Test
    public void roundTripPreservesBytes() throws Exception {
        byte[] raw = "hello, world!".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = MetadataCompression.compressGzip(raw);
        assertTrue(MetadataCompression.looksGzip(compressed));
        assertArrayEquals(raw, MetadataCompression.decompressGzip(compressed));
    }

    @Test
    public void decompressRejectsNonGzip() {
        try {
            MetadataCompression.decompressGzip(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            fail("expected IOException on bad gzip input");
        } catch (IOException expected) {
            // ok
        }
    }

    @Test
    public void compressesRepetitivePayloadWell() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb.append("activeLedger=").append(i).append(',');
        }
        byte[] raw = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = MetadataCompression.compressGzip(raw);
        // Sequential longs embedded in verbose text must compress dramatically.
        assertTrue(
                "expected >5x compression, got raw=" + raw.length + " compressed=" + compressed.length,
                compressed.length * 5 < raw.length);
    }
}
