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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TailableFileInputStreamTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testReadSeesAppendedData() throws IOException {
        Path file = folder.newFile("test.dat").toPath();

        // Write initial data
        Files.write(file, new byte[]{1, 2, 3});

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            // Read initial data
            assertEquals(1, in.read());
            assertEquals(2, in.read());
            assertEquals(3, in.read());
            // EOF
            assertEquals(-1, in.read());

            // Append more data to the file
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.APPEND)) {
                out.write(new byte[]{4, 5});
                out.flush();
            }

            // Read should now see the appended data
            assertEquals(4, in.read());
            assertEquals(5, in.read());
            assertEquals(-1, in.read());
        }
    }

    @Test
    public void testEmptyFileReturnsMinusOne() throws IOException {
        Path file = folder.newFile("empty.dat").toPath();

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            assertEquals(-1, in.read());
        }
    }

    @Test
    public void testBulkRead() throws IOException {
        Path file = folder.newFile("bulk.dat").toPath();

        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        Files.write(file, data);

        try (TailableFileInputStream in = new TailableFileInputStream(file, 64)) {
            byte[] buf = new byte[256];
            int totalRead = 0;
            int r;
            while ((r = in.read(buf, totalRead, buf.length - totalRead)) > 0) {
                totalRead += r;
            }
            assertEquals(256, totalRead);
            assertArrayEquals(data, buf);
        }
    }

    @Test
    public void testBulkReadSeesAppendedData() throws IOException {
        Path file = folder.newFile("bulkappend.dat").toPath();

        Files.write(file, new byte[]{10, 20, 30});

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            byte[] buf = new byte[3];
            assertEquals(3, in.read(buf, 0, 3));
            assertArrayEquals(new byte[]{10, 20, 30}, buf);

            // EOF on bulk read
            assertEquals(-1, in.read(buf, 0, 3));

            // Append
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.APPEND)) {
                out.write(new byte[]{40, 50});
                out.flush();
            }

            byte[] buf2 = new byte[2];
            assertEquals(2, in.read(buf2, 0, 2));
            assertArrayEquals(new byte[]{40, 50}, buf2);
        }
    }

    @Test
    public void testPosition() throws IOException {
        Path file = folder.newFile("pos.dat").toPath();
        Files.write(file, new byte[]{1, 2, 3, 4, 5});

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            assertEquals(0, in.position());
            in.read();
            assertEquals(1, in.position());
            in.read();
            in.read();
            assertEquals(3, in.position());
        }
    }

    @Test
    public void testMarkAndReset() throws IOException {
        Path file = folder.newFile("mark.dat").toPath();
        Files.write(file, new byte[]{1, 2, 3, 4, 5});

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            assertTrue(in.markSupported());
            assertEquals(1, in.read());
            assertEquals(2, in.read());

            // Mark at position 2
            in.mark(Integer.MAX_VALUE);

            assertEquals(3, in.read());
            assertEquals(4, in.read());

            // Reset back to position 2
            in.reset();

            // Should re-read bytes 3, 4, 5
            assertEquals(3, in.read());
            assertEquals(4, in.read());
            assertEquals(5, in.read());
            assertEquals(-1, in.read());
        }
    }

    @Test
    public void testMarkResetWithAppend() throws IOException {
        Path file = folder.newFile("markappend.dat").toPath();
        Files.write(file, new byte[]{1, 2, 3});

        try (TailableFileInputStream in = new TailableFileInputStream(file, 1024)) {
            assertEquals(1, in.read());

            // Mark at position 1
            in.mark(Integer.MAX_VALUE);

            assertEquals(2, in.read());
            assertEquals(3, in.read());
            // EOF - simulates partial entry
            assertEquals(-1, in.read());

            // Append more data (simulates writer finishing the entry)
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.APPEND)) {
                out.write(new byte[]{4, 5});
                out.flush();
            }

            // Reset to position 1 and re-read from there
            in.reset();
            assertEquals(2, in.read());
            assertEquals(3, in.read());
            assertEquals(4, in.read());
            assertEquals(5, in.read());
            assertEquals(-1, in.read());
        }
    }
}
