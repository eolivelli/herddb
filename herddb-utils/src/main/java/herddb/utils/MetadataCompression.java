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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * GZIP helpers for metadata payloads written to ZooKeeper or to checkpoint
 * files. Callers use this to keep the persisted form small when the payload
 * contains lists that grow with activity (active ledgers, active pages).
 *
 * <p>GZIP is deliberately chosen over LZ4: these paths are not hot (they run
 * once per ledger roll or per checkpoint), and the higher compression ratio
 * of Deflate at level 9 on repetitive long-id sequences is the whole point.
 * The two-byte GZIP magic (0x1f 0x8b) also makes it trivial for readers to
 * auto-detect compressed vs. legacy uncompressed content.
 */
public final class MetadataCompression {

    private static final int GZIP_MAGIC_0 = 0x1f;
    private static final int GZIP_MAGIC_1 = 0x8b;

    private MetadataCompression() {
    }

    /**
     * Return true when {@code data} starts with the 2-byte GZIP magic.
     * Callers use this to decide whether to run the payload through
     * {@link #decompressGzip(byte[])} or parse it directly (legacy path).
     */
    public static boolean looksGzip(byte[] data) {
        return data != null
                && data.length >= 2
                && (data[0] & 0xff) == GZIP_MAGIC_0
                && (data[1] & 0xff) == GZIP_MAGIC_1;
    }

    /**
     * GZIP-compress {@code raw} using {@link Deflater#BEST_COMPRESSION}.
     */
    public static byte[] compressGzip(byte[] raw) throws IOException {
        try (VisibleByteArrayOutputStream out = new VisibleByteArrayOutputStream(Math.max(32, raw.length / 2));
             BestCompressionGzipOutputStream gz = new BestCompressionGzipOutputStream(out)) {
            gz.write(raw);
            gz.finish();
            return out.toByteArray();
        }
    }

    private static final class BestCompressionGzipOutputStream extends GZIPOutputStream {
        BestCompressionGzipOutputStream(java.io.OutputStream out) throws IOException {
            super(out);
            this.def.setLevel(Deflater.BEST_COMPRESSION);
        }
    }

    /**
     * GZIP-decompress {@code data}. Throws {@link IOException} if the input
     * is not a valid GZIP stream.
     */
    public static byte[] decompressGzip(byte[] data) throws IOException {
        try (VisibleByteArrayOutputStream out = new VisibleByteArrayOutputStream(Math.max(32, data.length * 2));
             ByteArrayInputStream bytes = new ByteArrayInputStream(data);
             GZIPInputStream gz = new GZIPInputStream(bytes)) {
            byte[] buffer = new byte[4096];
            int n;
            while ((n = gz.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        }
    }
}
