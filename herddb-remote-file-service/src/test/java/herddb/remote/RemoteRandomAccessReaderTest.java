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

package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link RemoteRandomAccessReader}: block buffering, seek, cross-block reads, int/float/long decode.
 */
public class RemoteRandomAccessReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private static final int BLOCK_SIZE = 64;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    /** Build a byte array where byte[i] = (byte)(i & 0xFF). */
    private static byte[] seqBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) b[i] = (byte) (i & 0xFF);
        return b;
    }

    @Test
    public void testSeekAndReadFully() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 2 + 20);
        client.writeMultipartFile("ts/idx/graph", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/graph", data.length, BLOCK_SIZE);
        reader.seek(10);
        assertEquals(10, reader.getPosition());

        byte[] buf = new byte[5];
        reader.readFully(buf);
        for (int i = 0; i < 5; i++) assertEquals((byte) (10 + i), buf[i]);
        assertEquals(15, reader.getPosition());
    }

    @Test
    public void testCrossBlockRead() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE * 2 + 10);
        client.writeMultipartFile("ts/idx/cross", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/cross", data.length, BLOCK_SIZE);
        // Seek to 4 bytes before end of first block
        reader.seek(BLOCK_SIZE - 4);
        byte[] buf = new byte[8]; // crosses block boundary
        reader.readFully(buf);
        for (int i = 0; i < 8; i++) assertEquals((byte) (BLOCK_SIZE - 4 + i), buf[i]);
    }

    @Test
    public void testReadInt() throws Exception {
        // Write a known int value big-endian
        int value = 0x01020304;
        byte[] block = new byte[BLOCK_SIZE];
        block[0] = 0x01; block[1] = 0x02; block[2] = 0x03; block[3] = 0x04;
        client.writeMultipartFile("ts/idx/int", new ByteArrayInputStream(block), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/int", BLOCK_SIZE, BLOCK_SIZE);
        reader.seek(0);
        assertEquals(value, reader.readInt());
    }

    @Test
    public void testReadFloat() throws Exception {
        float expected = 3.14f;
        byte[] block = new byte[BLOCK_SIZE];
        int bits = Float.floatToIntBits(expected);
        block[0] = (byte) (bits >>> 24);
        block[1] = (byte) (bits >>> 16);
        block[2] = (byte) (bits >>> 8);
        block[3] = (byte) bits;
        client.writeMultipartFile("ts/idx/float", new ByteArrayInputStream(block), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/float", BLOCK_SIZE, BLOCK_SIZE);
        reader.seek(0);
        assertEquals(expected, reader.readFloat(), 0.0f);
    }

    @Test
    public void testReadLong() throws Exception {
        long expected = 0x0102030405060708L;
        byte[] block = new byte[BLOCK_SIZE];
        for (int i = 0; i < 8; i++) block[i] = (byte) (expected >>> (56 - i * 8));
        client.writeMultipartFile("ts/idx/long", new ByteArrayInputStream(block), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/long", BLOCK_SIZE, BLOCK_SIZE);
        reader.seek(0);
        assertEquals(expected, reader.readLong());
    }

    @Test
    public void testSupplierCreatesIndependentReaders() throws Exception {
        byte[] data = seqBytes(BLOCK_SIZE + 10);
        client.writeMultipartFile("ts/idx/supplier", new ByteArrayInputStream(data), BLOCK_SIZE);

        ReaderSupplier supplier = new RemoteRandomAccessReader.Supplier(client, "ts/idx/supplier", data.length, BLOCK_SIZE);
        try (RandomAccessReader r1 = supplier.get();
             RandomAccessReader r2 = supplier.get()) {
            r1.seek(0);
            r2.seek(5);
            byte[] b1 = new byte[3];
            byte[] b2 = new byte[3];
            r1.readFully(b1);
            r2.readFully(b2);
            assertArrayEquals(new byte[]{0, 1, 2}, b1);
            assertArrayEquals(new byte[]{5, 6, 7}, b2);
        }
    }

    @Test
    public void testLength() throws Exception {
        byte[] data = seqBytes(150);
        client.writeMultipartFile("ts/idx/len", new ByteArrayInputStream(data), BLOCK_SIZE);

        RemoteRandomAccessReader reader = new RemoteRandomAccessReader(client, "ts/idx/len", data.length, BLOCK_SIZE);
        assertEquals(150, reader.length());
    }
}
