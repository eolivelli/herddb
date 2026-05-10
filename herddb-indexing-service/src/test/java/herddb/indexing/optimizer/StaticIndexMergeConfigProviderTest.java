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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Test;

/**
 * Unit tests for {@link StaticIndexMergeConfigProvider} (issue #516).
 *
 * <p>Verifies:
 * <ul>
 *   <li>The constructor accepts an {@link IndexMergeConfig} and returns it on
 *       every {@code getMergeConfig} call, regardless of the index identity.</li>
 *   <li>Passing {@code null} throws {@link NullPointerException}.</li>
 * </ul>
 *
 * <p>Note: this provider is a test-only helper. Production code uses
 * {@link RemoteMetadataIndexMergeConfigProvider}, which reads per-index build
 * parameters from checkpoint metadata on the remote file server.
 */
public class StaticIndexMergeConfigProviderTest {

    @Test
    public void returnsSuppliedConfigOnEveryCall() throws Exception {
        IndexMergeConfig expected = new IndexMergeConfig(
                /* graphM */ 16, /* beamWidth */ 64,
                /* neighborOverflow */ 1.2f, /* alpha */ 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);

        StaticIndexMergeConfigProvider provider = new StaticIndexMergeConfigProvider(expected);

        // Must return the exact same instance on every call, regardless of parameters.
        IndexMergeConfig cfgA = provider.getMergeConfig("ts1", "table1", "idx1", "uuid-A");
        IndexMergeConfig cfgB = provider.getMergeConfig("ts2", "table2", "idx2", "uuid-B");

        assertSame("must return the identical IndexMergeConfig instance", expected, cfgA);
        assertSame("must return the identical IndexMergeConfig instance", expected, cfgB);
    }

    @Test
    public void throwsNpeOnNullConfig() {
        try {
            new StaticIndexMergeConfigProvider(null);
            fail("expected NullPointerException for null config");
        } catch (NullPointerException e) {
            // Expected.
        }
    }
}
