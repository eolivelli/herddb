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

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

/**
 * Unit tests for the pod-role detection precedence rules (step 1):
 * explicit config &gt; env-var ordinal &gt; hostname regex &gt; default WORKER.
 */
public class OptimizerRoleTest {

    private static OptimizerConfiguration cfg(Properties props) {
        return new OptimizerConfiguration(props);
    }

    private static Map<String, String> env(String... kvs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i + 1 < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    @Test
    public void explicitTrueWinsRegardlessOfEnv() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER, "true");
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(p), env("POD_ORDINAL", "5", "HOSTNAME", "x-3")));
    }

    @Test
    public void explicitFalseWinsRegardlessOfEnv() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ROLE_IS_LEADER, "false");
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(p), env("POD_ORDINAL", "0", "HOSTNAME", "x-0")));
    }

    @Test
    public void autoFallsThroughToEnv_ordinalZeroIsLeader() {
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(new Properties()), env("POD_ORDINAL", "0")));
    }

    @Test
    public void autoFallsThroughToEnv_ordinalNonZeroIsWorker() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()), env("POD_ORDINAL", "3")));
    }

    @Test
    public void envVarNameIsConfigurable() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ROLE_POD_ORDINAL_ENV, "MY_ORDINAL");
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(p), env("MY_ORDINAL", "0", "POD_ORDINAL", "9")));
    }

    @Test
    public void unparseableEnvFallsThroughToHostname() {
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(new Properties()),
                        env("POD_ORDINAL", "not-a-number", "HOSTNAME", "indexer-0")));
    }

    @Test
    public void hostnameRegexExtractsOrdinalZero() {
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(new Properties()),
                        env("HOSTNAME", "release-index-optimizer-0")));
    }

    @Test
    public void hostnameRegexExtractsOrdinalNonZero() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()),
                        env("HOSTNAME", "release-index-optimizer-7")));
    }

    @Test
    public void hostnameWithoutOrdinalDefaultsToWorker() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()), env("HOSTNAME", "no-digits-here")));
    }

    @Test
    public void noSignalsAtAllDefaultsToWorker() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()), env()));
    }

    @Test
    public void customHostnameRegex() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX, "^pod_(\\d+)_.*");
        assertEquals(OptimizerRole.LEADER,
                OptimizerRole.detect(cfg(p), env("HOSTNAME", "pod_0_us-east")));
    }

    @Test
    public void invalidCustomRegexDefaultsToWorker() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_ROLE_HOSTNAME_ORDINAL_REGEX, "([unclosed");
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(p), env("HOSTNAME", "anything-0")));
    }

    @Test
    public void negativeOrdinalIsIgnored() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()), env("POD_ORDINAL", "-1")));
    }

    @Test
    public void nullEnvTreatedAsEmpty() {
        assertEquals(OptimizerRole.WORKER,
                OptimizerRole.detect(cfg(new Properties()), null));
    }
}
