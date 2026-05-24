/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.proto;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Encode/decode roundtrip tests for the file-server PDU codec helpers
 * introduced for issue #425. Each test writes a request or response, decodes
 * it back via {@link PduCodec#decodePdu(ByteBuf)}, asserts the {@code type}
 * and {@code flags} bytes match, and reads the payload back through the
 * matching {@code readXxx} helper.
 */
public class FileServerPduCodecTest {

    private static byte[] readByteArray(ByteBuf buf) {
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        return out;
    }

    @Test
    public void writeFileRoundtrip() throws IOException {
        byte[] payload = new byte[1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xff);
        }
        ByteBuf buf = PduCodec.WriteFileRequest.write(42L, "tablespace1/uuid/data/page1.page",
                payload, 0, payload.length);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_WRITE_FILE, pdu.type);
            assertTrue(pdu.isRequest());
            assertFalse(pdu.isResponse());
            assertEquals(42L, pdu.messageId);
            assertEquals("tablespace1/uuid/data/page1.page", PduCodec.WriteFileRequest.readPath(pdu));
            ByteBuf content = PduCodec.WriteFileRequest.readContent(pdu);
            try {
                assertArrayEquals(payload, readByteArray(content));
            } finally {
                content.release();
            }
        }
    }

    @Test
    public void writeFileFromByteBufRoundtrip() throws IOException {
        byte[] payload = "hello-bytebuf".getBytes();
        ByteBuf src = Unpooled.wrappedBuffer(payload);
        ByteBuf buf = PduCodec.WriteFileRequest.write(7L, "foo", src);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_WRITE_FILE, pdu.type);
            assertEquals("foo", PduCodec.WriteFileRequest.readPath(pdu));
            ByteBuf content = PduCodec.WriteFileRequest.readContent(pdu);
            try {
                assertArrayEquals(payload, readByteArray(content));
            } finally {
                content.release();
            }
        }
    }

    @Test
    public void writeFileResponseRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.WriteFileResponse.write(99L, 12345L);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_WRITE_FILE, pdu.type);
            assertTrue(pdu.isResponse());
            assertEquals(99L, pdu.messageId);
            assertEquals(12345L, PduCodec.WriteFileResponse.readWrittenSize(pdu));
        }
    }

    @Test
    public void prefetchFileRangeRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.PrefetchFileRangeRequest.write(
                11L, "ts/uuid/multipart/graph", 4096L, 65536, 4 * 1024 * 1024);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_PREFETCH_FILE_RANGE, pdu.type);
            assertTrue(pdu.isRequest());
            assertEquals(11L, pdu.messageId);
            assertEquals("ts/uuid/multipart/graph",
                    PduCodec.PrefetchFileRangeRequest.readPath(pdu));
            assertEquals(4096L, PduCodec.PrefetchFileRangeRequest.readOffset(pdu));
            assertEquals(65536, PduCodec.PrefetchFileRangeRequest.readLength(pdu));
            assertEquals(4 * 1024 * 1024,
                    PduCodec.PrefetchFileRangeRequest.readBlockSize(pdu));
        }
    }

    @Test
    public void prefetchFileRangeResponseOkRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.PrefetchFileRangeResponse.writeOk(42L);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_PREFETCH_FILE_RANGE, pdu.type);
            assertTrue(pdu.isResponse());
            assertEquals(42L, pdu.messageId);
            assertEquals(PduCodec.PrefetchFileRangeResponse.STATUS_OK,
                    PduCodec.PrefetchFileRangeResponse.readStatus(pdu));
        }
    }

    @Test
    public void prefetchFileRangeResponseNotFoundRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.PrefetchFileRangeResponse.writeNotFound(43L);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(PduCodec.PrefetchFileRangeResponse.STATUS_NOT_FOUND,
                    PduCodec.PrefetchFileRangeResponse.readStatus(pdu));
        }
    }

    @Test
    public void prefetchFileRangeResponseErrorRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.PrefetchFileRangeResponse.writeError(44L, "S3 5xx");
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(PduCodec.PrefetchFileRangeResponse.STATUS_ERROR,
                    PduCodec.PrefetchFileRangeResponse.readStatus(pdu));
            assertEquals("S3 5xx",
                    PduCodec.PrefetchFileRangeResponse.readErrorMessage(pdu));
        }
    }

    @Test
    public void readFileRoundtrip() throws IOException {
        ByteBuf req = PduCodec.ReadFileRequest.write(1L, "a/b/c");
        try (Pdu pdu = PduCodec.decodePdu(req)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE, pdu.type);
            assertTrue(pdu.isRequest());
            assertEquals(1L, pdu.messageId);
            assertEquals("a/b/c", PduCodec.ReadFileRequest.readPath(pdu));
        }
    }

    @Test
    public void readFileResponseFoundRoundtrip() throws IOException {
        byte[] payload = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        ByteBuf buf = PduCodec.ReadFileResponse.writeFound(2L, payload, 0, payload.length);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE, pdu.type);
            assertTrue(pdu.isResponse());
            assertTrue(PduCodec.ReadFileResponse.readFound(pdu));
            assertEquals(payload.length, PduCodec.ReadFileResponse.readContentLength(pdu));
            ByteBuf content = PduCodec.ReadFileResponse.readContent(pdu);
            try {
                assertArrayEquals(payload, readByteArray(content));
            } finally {
                content.release();
            }
        }
    }

    @Test
    public void readFileResponseNotFoundRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.ReadFileResponse.writeNotFound(3L);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE, pdu.type);
            assertFalse(PduCodec.ReadFileResponse.readFound(pdu));
            assertEquals(0, PduCodec.ReadFileResponse.readContentLength(pdu));
        }
    }

    @Test
    public void readFileRangeRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.ReadFileRangeRequest.write(5L, "p/q", 1024L, 4096, 8192);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE_RANGE, pdu.type);
            assertEquals("p/q", PduCodec.ReadFileRangeRequest.readPath(pdu));
            assertEquals(1024L, PduCodec.ReadFileRangeRequest.readOffset(pdu));
            assertEquals(4096, PduCodec.ReadFileRangeRequest.readLength(pdu));
            assertEquals(8192, PduCodec.ReadFileRangeRequest.readBlockSize(pdu));
        }
    }

    @Test
    public void readFileRangeResponseFoundRoundtrip() throws IOException {
        byte[] payload = new byte[]{9, 8, 7, 6};
        ByteBuf buf = PduCodec.ReadFileRangeResponse.writeFound(6L, payload, 0, payload.length);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE_RANGE, pdu.type);
            assertTrue(PduCodec.ReadFileRangeResponse.readFound(pdu));
            assertEquals(payload.length, PduCodec.ReadFileRangeResponse.readContentLength(pdu));
            ByteBuf content = PduCodec.ReadFileRangeResponse.readContent(pdu);
            try {
                assertArrayEquals(payload, readByteArray(content));
            } finally {
                content.release();
            }
        }
    }

    @Test
    public void readFileRangeResponseNotFoundRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.ReadFileRangeResponse.writeNotFound(7L);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_READ_FILE_RANGE, pdu.type);
            assertFalse(PduCodec.ReadFileRangeResponse.readFound(pdu));
        }
    }

    @Test
    public void deleteFileRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.DeleteFileRequest.write(8L, "to-delete");
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_DELETE_FILE, pdu.type);
            assertEquals("to-delete", PduCodec.DeleteFileRequest.readPath(pdu));
        }
    }

    @Test
    public void deleteFileResponseRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.DeleteFileResponse.write(8L, true);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertTrue(PduCodec.DeleteFileResponse.readDeleted(pdu));
        }
        ByteBuf buf2 = PduCodec.DeleteFileResponse.write(9L, false);
        try (Pdu pdu = PduCodec.decodePdu(buf2)) {
            assertFalse(PduCodec.DeleteFileResponse.readDeleted(pdu));
        }
    }

    @Test
    public void deleteFilesBatchRoundtrip() throws IOException {
        List<String> paths = Arrays.asList("a", "b/c", "d/e/f");
        ByteBuf buf = PduCodec.DeleteFilesRequest.write(10L, paths);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_DELETE_FILES, pdu.type);
            assertEquals(paths, PduCodec.DeleteFilesRequest.readPaths(pdu));
        }
    }

    @Test
    public void deleteFilesResponseRoundtrip() throws IOException {
        List<PduCodec.DeleteFilesResponse.Outcome> outcomes = Arrays.asList(
                new PduCodec.DeleteFilesResponse.Outcome("a", true, ""),
                new PduCodec.DeleteFilesResponse.Outcome("b", false, ""),
                new PduCodec.DeleteFilesResponse.Outcome("c", false, "boom"));
        ByteBuf buf = PduCodec.DeleteFilesResponse.write(10L, outcomes);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            List<PduCodec.DeleteFilesResponse.Outcome> read = PduCodec.DeleteFilesResponse.readOutcomes(pdu);
            assertEquals(3, read.size());
            assertEquals("a", read.get(0).path);
            assertTrue(read.get(0).deleted);
            assertEquals("", read.get(0).error);
            assertEquals("b", read.get(1).path);
            assertFalse(read.get(1).deleted);
            assertEquals("", read.get(1).error);
            assertEquals("c", read.get(2).path);
            assertFalse(read.get(2).deleted);
            assertEquals("boom", read.get(2).error);
        }
    }

    @Test
    public void deleteFilesBatchEmpty() throws IOException {
        ByteBuf buf = PduCodec.DeleteFilesRequest.write(11L, java.util.Collections.emptyList());
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertTrue(PduCodec.DeleteFilesRequest.readPaths(pdu).isEmpty());
        }
        ByteBuf buf2 = PduCodec.DeleteFilesResponse.write(11L, java.util.Collections.emptyList());
        try (Pdu pdu = PduCodec.decodePdu(buf2)) {
            assertTrue(PduCodec.DeleteFilesResponse.readOutcomes(pdu).isEmpty());
        }
    }

    @Test
    public void listFilesRoundtrip() throws IOException {
        ByteBuf req = PduCodec.ListFilesRequest.write(12L, "tbl/uuid/");
        try (Pdu pdu = PduCodec.decodePdu(req)) {
            assertEquals(Pdu.TYPE_FS_LIST_FILES, pdu.type);
            assertEquals("tbl/uuid/", PduCodec.ListFilesRequest.readPrefix(pdu));
        }
    }

    @Test
    public void listFilesResponseRoundtrip() throws IOException {
        List<String> paths = Arrays.asList("a/b/c.page", "a/b/d.page", "a/b/e.page");
        ByteBuf buf = PduCodec.ListFilesResponse.write(12L, paths);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_LIST_FILES, pdu.type);
            assertTrue(pdu.isResponse());
            assertEquals(paths, PduCodec.ListFilesResponse.readPaths(pdu));
        }
    }

    @Test
    public void listFilesResponseEmptyRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.ListFilesResponse.write(13L, java.util.Collections.emptyList());
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertTrue(PduCodec.ListFilesResponse.readPaths(pdu).isEmpty());
        }
    }

    @Test
    public void deleteByPrefixRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.DeleteByPrefixRequest.write(14L, "tbl/uuid/data/");
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(Pdu.TYPE_FS_DELETE_BY_PREFIX, pdu.type);
            assertEquals("tbl/uuid/data/", PduCodec.DeleteByPrefixRequest.readPrefix(pdu));
        }
    }

    @Test
    public void deleteByPrefixResponseRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.DeleteByPrefixResponse.write(14L, 17);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(17, PduCodec.DeleteByPrefixResponse.readDeletedCount(pdu));
        }
    }

    @Test
    public void getServerInfoRoundtrip() throws IOException {
        ByteBuf req = PduCodec.GetServerInfoRequest.write(20L);
        try (Pdu pdu = PduCodec.decodePdu(req)) {
            assertEquals(Pdu.TYPE_FS_GET_SERVER_INFO, pdu.type);
            assertTrue(pdu.isRequest());
        }
    }

    @Test
    public void getServerInfoResponseRoundtrip() throws IOException {
        PduCodec.GetServerInfoResponse.Info info = new PduCodec.GetServerInfoResponse.Info();
        info.host = "10.0.0.1";
        info.port = 9846;
        info.storageMode = "s3";
        info.jvmHeapUsedBytes = 1L << 30;
        info.jvmHeapMaxBytes = 1L << 32;
        info.diskCacheMaxBytes = 1L << 33;
        info.diskCacheHitCount = 1234;
        info.diskCacheMissCount = 56;
        info.diskCacheEvictionCount = 7;
        info.diskCacheHitBytes = 1L << 28;
        info.diskCacheMissBytes = 1L << 24;
        info.diskCacheEstimatedEntries = 99;
        info.blockCacheMaxBytes = 1L << 31;
        info.blockCacheEstimatedBytes = 1L << 27;
        info.blockCacheEstimatedEntries = 42;
        info.blockCacheHits = 100;
        info.blockCacheMisses = 200;
        info.blockCacheEvictions = 3;
        ByteBuf buf = PduCodec.GetServerInfoResponse.write(20L, info);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            PduCodec.GetServerInfoResponse.Info read = PduCodec.GetServerInfoResponse.read(pdu);
            assertEquals(info.host, read.host);
            assertEquals(info.port, read.port);
            assertEquals(info.storageMode, read.storageMode);
            assertEquals(info.jvmHeapUsedBytes, read.jvmHeapUsedBytes);
            assertEquals(info.jvmHeapMaxBytes, read.jvmHeapMaxBytes);
            assertEquals(info.diskCacheMaxBytes, read.diskCacheMaxBytes);
            assertEquals(info.diskCacheHitCount, read.diskCacheHitCount);
            assertEquals(info.diskCacheMissCount, read.diskCacheMissCount);
            assertEquals(info.diskCacheEvictionCount, read.diskCacheEvictionCount);
            assertEquals(info.diskCacheHitBytes, read.diskCacheHitBytes);
            assertEquals(info.diskCacheMissBytes, read.diskCacheMissBytes);
            assertEquals(info.diskCacheEstimatedEntries, read.diskCacheEstimatedEntries);
            assertEquals(info.blockCacheMaxBytes, read.blockCacheMaxBytes);
            assertEquals(info.blockCacheEstimatedBytes, read.blockCacheEstimatedBytes);
            assertEquals(info.blockCacheEstimatedEntries, read.blockCacheEstimatedEntries);
            assertEquals(info.blockCacheHits, read.blockCacheHits);
            assertEquals(info.blockCacheMisses, read.blockCacheMisses);
            assertEquals(info.blockCacheEvictions, read.blockCacheEvictions);
        }
    }

    @Test
    public void resizeDiskCacheRoundtrip() throws IOException {
        ByteBuf req = PduCodec.ResizeDiskCacheRequest.write(21L, 1L << 34);
        try (Pdu pdu = PduCodec.decodePdu(req)) {
            assertEquals(Pdu.TYPE_FS_RESIZE_DISK_CACHE, pdu.type);
            assertEquals(1L << 34, PduCodec.ResizeDiskCacheRequest.readNewMaxBytes(pdu));
        }
    }

    @Test
    public void resizeDiskCacheResponseRoundtrip() throws IOException {
        ByteBuf buf = PduCodec.ResizeDiskCacheResponse.write(21L, 1L << 30, 1L << 34);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(1L << 30, PduCodec.ResizeDiskCacheResponse.readPreviousMaxBytes(pdu));
            assertEquals(1L << 34, PduCodec.ResizeDiskCacheResponse.readNewMaxBytes(pdu));
        }
    }

    @Test
    public void emptyContentRoundtrip() throws IOException {
        // WRITE_FILE with zero-length content (e.g. truncate). Several existing
        // file-server tests deliberately exercise this case (empty page, empty
        // metadata), so the codec must handle it without an off-by-one.
        ByteBuf buf = PduCodec.WriteFileRequest.write(50L, "empty", new byte[0], 0, 0);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals("empty", PduCodec.WriteFileRequest.readPath(pdu));
            ByteBuf content = PduCodec.WriteFileRequest.readContent(pdu);
            try {
                assertEquals(0, content.readableBytes());
            } finally {
                content.release();
            }
        }
        ByteBuf foundBuf = PduCodec.ReadFileResponse.writeFound(50L, new byte[0], 0, 0);
        try (Pdu pdu = PduCodec.decodePdu(foundBuf)) {
            assertTrue(PduCodec.ReadFileResponse.readFound(pdu));
            assertEquals(0, PduCodec.ReadFileResponse.readContentLength(pdu));
        }
    }

    @Test(expected = IOException.class)
    public void decodePduRejectsUnknownVersion() throws IOException {
        // Build a 12-byte buffer with version=99 (unsupported). decodePdu must
        // refuse rather than silently constructing a Pdu with garbage fields.
        ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(12);
        buf.writeByte(99);                     // bogus version
        buf.writeByte(Pdu.FLAGS_ISREQUEST);
        buf.writeByte(Pdu.TYPE_FS_WRITE_FILE);
        buf.writeLong(7L);                     // messageId
        buf.writeByte(0);
        try {
            PduCodec.decodePdu(buf);
        } finally {
            buf.release();
        }
    }

    @Test
    public void truncatedDeleteFilesResponseFailsCleanly() throws IOException {
        // DeleteFilesResponse declares 2 outcomes but only writes payload
        // for one. readOutcomes must throw, not return a partially-populated
        // list, when the second outcome's string-length runs past EOF.
        ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer();
        buf.writeByte(PduCodec.VERSION_3);
        buf.writeByte(Pdu.FLAGS_ISRESPONSE);
        buf.writeByte(Pdu.TYPE_FS_DELETE_FILES);
        buf.writeLong(42L);
        buf.writeInt(2);                        // count = 2
        // First outcome (intact): path="ok", deleted=true, error=""
        herddb.utils.ByteBufUtils.writeString(buf, "ok");
        buf.writeByte(1);
        herddb.utils.ByteBufUtils.writeString(buf, "");
        // Second outcome: nothing — frame ends here.
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            try {
                PduCodec.DeleteFilesResponse.readOutcomes(pdu);
                org.junit.Assert.fail("Expected exception on truncated response");
            } catch (RuntimeException expected) {
                // pass — codec must throw when the buffer ends mid-record.
            }
        }
    }

    @Test
    public void largeContentRoundtrip() throws IOException {
        // 5 MiB body — exceeds the default gRPC inbound message limit, makes
        // sure we are not accidentally writing a vint/short for the length.
        byte[] payload = new byte[5 * 1024 * 1024];
        Arrays.fill(payload, (byte) 'X');
        ByteBuf buf = PduCodec.WriteFileRequest.write(60L, "big", payload, 0, payload.length);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            ByteBuf content = PduCodec.WriteFileRequest.readContent(pdu);
            try {
                assertEquals(payload.length, content.readableBytes());
                assertArrayEquals(payload, readByteArray(content));
            } finally {
                content.release();
            }
        }
    }
}
