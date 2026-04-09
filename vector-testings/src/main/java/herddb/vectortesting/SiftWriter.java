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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Streaming writer for SIFT binary formats (FVECS and IVECS).
 * Each record is: [4-byte little-endian int: dimension] [dimension values].
 * FVECS values are 4-byte little-endian floats; IVECS values are 4-byte little-endian ints.
 */
public class SiftWriter implements Closeable {

    private final BufferedOutputStream out;

    public SiftWriter(File outputFile) throws IOException {
        this.out = new BufferedOutputStream(new FileOutputStream(outputFile), 256 * 1024);
    }

    public SiftWriter(OutputStream outputStream) throws IOException {
        this.out = new BufferedOutputStream(outputStream, 256 * 1024);
    }

    public void writeFvec(float[] vector) throws IOException {
        int dim = vector.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + dim * 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(dim);
        for (float v : vector) {
            buf.putFloat(v);
        }
        out.write(buf.array());
    }

    public void writeIvec(int[] ids) throws IOException {
        int dim = ids.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + dim * 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(dim);
        for (int id : ids) {
            buf.putInt(id);
        }
        out.write(buf.array());
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
