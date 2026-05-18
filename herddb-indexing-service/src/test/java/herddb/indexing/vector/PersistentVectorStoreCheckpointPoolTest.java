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

package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import java.lang.reflect.Field;
import java.util.concurrent.ForkJoinPool;
import org.junit.Test;

/**
 * Verifies that the JVM-wide checkpoint {@link ForkJoinPool} parallelism is
 * driven by the {@code herddb.vectorindex.checkpointThreads} system property
 * (with a default of {@code max(1, availableProcessors() / 2)}).
 */
public class PersistentVectorStoreCheckpointPoolTest {

    @Test
    public void checkpointPoolParallelismMatchesSystemProperty() throws Exception {
        int defaultSize = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        int expected = Math.max(1,
                Integer.getInteger("herddb.vectorindex.checkpointThreads", defaultSize));

        Field f = PersistentVectorStore.class.getDeclaredField("CHECKPOINT_POOL");
        f.setAccessible(true);
        ForkJoinPool pool = (ForkJoinPool) f.get(null);

        assertEquals(expected, pool.getParallelism());
    }
}
