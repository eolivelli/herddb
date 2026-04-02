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

import herddb.utils.Bytes;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for vector stores.
 * Implementations store vectors keyed by primary key and support similarity search.
 *
 * @author enrico.olivelli
 */
public abstract class AbstractVectorStore implements AutoCloseable {

    protected final String vectorColumnName;

    protected AbstractVectorStore(String vectorColumnName) {
        this.vectorColumnName = vectorColumnName;
    }

    public String getVectorColumnName() {
        return vectorColumnName;
    }

    public abstract void addVector(Bytes pk, float[] vector);

    public abstract void removeVector(Bytes pk);

    public abstract int size();

    public abstract List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK);

    public abstract long estimatedMemoryUsageBytes();

    public abstract void start() throws Exception;

    @Override
    public void close() throws Exception {
    }
}
