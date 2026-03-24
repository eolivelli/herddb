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

package herddb.index.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentedMappedReaderTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testBasicReadOperations() throws IOException {
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeInt(42);
            dos.writeFloat(3.14f);
            dos.writeLong(123456789L);
            dos.write(new byte[]{1, 2, 3, 4});
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file)) {
            assertEquals(42, reader.readInt());
            assertEquals(3.14f, reader.readFloat(), 0.0f);
            assertEquals(123456789L, reader.readLong());
            byte[] buf = new byte[4];
            reader.readFully(buf);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, buf);
        }
    }

    @Test
    public void testSeek() throws IOException {
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeInt(100);
            dos.writeInt(200);
            dos.writeInt(300);
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file)) {
            assertEquals(0, reader.getPosition());
            assertEquals(100, reader.readInt());
            assertEquals(4, reader.getPosition());

            reader.seek(8);
            assertEquals(300, reader.readInt());

            reader.seek(0);
            assertEquals(100, reader.readInt());
        }
    }

    @Test
    public void testLength() throws IOException {
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeInt(1);
            dos.writeInt(2);
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file)) {
            assertEquals(8, reader.length());
        }
    }

    @Test
    public void testMultipleSegments() throws IOException {
        // Use a tiny segment size (32 bytes) to force multiple segments
        int segmentSize = 32;
        int numInts = 100; // 400 bytes = ~13 segments
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            for (int i = 0; i < numInts; i++) {
                dos.writeInt(i);
            }
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            for (int i = 0; i < numInts; i++) {
                assertEquals("int at position " + i, i, reader.readInt());
            }
        }
    }

    @Test
    public void testReadStraddlingSegmentBoundary() throws IOException {
        // Segment size = 10 bytes: an int (4 bytes) starting at offset 8 straddles the boundary
        int segmentSize = 10;
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeInt(111);  // bytes 0-3
            dos.writeInt(222);  // bytes 4-7
            dos.writeInt(333);  // bytes 8-11 — straddles segment boundary at 10
            dos.writeInt(444);  // bytes 12-15
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            assertEquals(111, reader.readInt());
            assertEquals(222, reader.readInt());
            assertEquals(333, reader.readInt());
            assertEquals(444, reader.readInt());
        }
    }

    @Test
    public void testReadFloatArrayStraddlingBoundary() throws IOException {
        int segmentSize = 10;
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            for (int i = 0; i < 10; i++) {
                dos.writeFloat(i * 1.5f);
            }
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            float[] result = new float[10];
            reader.readFully(result);
            for (int i = 0; i < 10; i++) {
                assertEquals(i * 1.5f, result[i], 0.0f);
            }
        }
    }

    @Test
    public void testReadFullyByteBufferStraddlingBoundary() throws IOException {
        int segmentSize = 10;
        Path file = tempFolder.newFile("test.bin").toPath();
        byte[] data = new byte[25];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i + 1);
        }
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            fos.write(data);
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            ByteBuffer dest = ByteBuffer.allocate(25);
            reader.readFully(dest);
            dest.flip();
            byte[] result = new byte[25];
            dest.get(result);
            assertArrayEquals(data, result);
        }
    }

    @Test
    public void testSeekAcrossSegments() throws IOException {
        int segmentSize = 16;
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            for (int i = 0; i < 20; i++) {
                dos.writeInt(i * 10);
            }
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            // Read from second segment (offset 16)
            reader.seek(16);
            assertEquals(40, reader.readInt());  // int at index 4

            // Jump back to first segment
            reader.seek(0);
            assertEquals(0, reader.readInt());

            // Jump to last int (offset 76)
            reader.seek(76);
            assertEquals(190, reader.readInt());
        }
    }

    @Test
    public void testReadLongArrayStraddlingBoundary() throws IOException {
        int segmentSize = 10;
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeLong(Long.MAX_VALUE);
            dos.writeLong(Long.MIN_VALUE);
            dos.writeLong(0L);
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            long[] result = new long[3];
            reader.readFully(result);
            assertEquals(Long.MAX_VALUE, result[0]);
            assertEquals(Long.MIN_VALUE, result[1]);
            assertEquals(0L, result[2]);
        }
    }

    @Test
    public void testReadIntArray() throws IOException {
        int segmentSize = 10;
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            for (int i = 0; i < 8; i++) {
                dos.writeInt(i + 100);
            }
        }

        try (SegmentedMappedReader reader = new SegmentedMappedReader(file, segmentSize)) {
            int[] result = new int[8];
            reader.read(result, 0, 8);
            for (int i = 0; i < 8; i++) {
                assertEquals(i + 100, result[i]);
            }
        }
    }

    @Test
    public void testSupplierReturnsIndependentReaders() throws IOException {
        Path file = tempFolder.newFile("test.bin").toPath();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            dos.writeInt(10);
            dos.writeInt(20);
            dos.writeInt(30);
        }

        try (SegmentedMappedReader.Supplier supplier = new SegmentedMappedReader.Supplier(file)) {
            try (RandomAccessReader r1 = supplier.get();
                 RandomAccessReader r2 = supplier.get()) {
                // Both readers start at position 0
                assertEquals(10, r1.readInt());
                assertEquals(10, r2.readInt());

                // Advancing one doesn't affect the other
                assertEquals(20, r1.readInt());
                assertEquals(20, r2.readInt());

                // Seek on one doesn't affect the other
                r1.seek(0);
                assertEquals(10, r1.readInt());
                assertEquals(30, r2.readInt());
            }
        }
    }
}
