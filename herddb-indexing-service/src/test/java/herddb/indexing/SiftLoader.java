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

package herddb.indexing;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads SIFT-format .fvecs and .ivecs files.
 */
public class SiftLoader {

    public static List<float[]> readFvecs(InputStream input) {
        List<float[]> vectors = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(input))) {
            while (dis.available() > 0) {
                int dimension = Integer.reverseBytes(dis.readInt());
                byte[] buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
                float[] vector = new float[dimension];
                byteBuffer.asFloatBuffer().get(vector);
                vectors.add(vector);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return vectors;
    }

    public static List<List<Integer>> readIvecs(InputStream input) {
        List<List<Integer>> groundTruth = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(input))) {
            while (dis.available() > 0) {
                int numNeighbors = Integer.reverseBytes(dis.readInt());
                List<Integer> neighbors = new ArrayList<>(numNeighbors);
                for (int i = 0; i < numNeighbors; i++) {
                    neighbors.add(Integer.reverseBytes(dis.readInt()));
                }
                groundTruth.add(neighbors);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return groundTruth;
    }
}
