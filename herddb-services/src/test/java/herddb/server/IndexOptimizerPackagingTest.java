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
package herddb.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.Test;

/**
 * Smoke tests for the index-optimizer packaging additions in the herddb-services
 * module: the {@code bin/service} launcher must dispatch the new
 * {@code index-optimizer} case to {@link herddb.indexing.optimizer.IndexOptimizerMain},
 * and the default {@code conf/indexoptimizer.properties} must be shipped with
 * the expected keys.
 *
 * <p>The test reads the source files directly from the module's
 * {@code src/main/resources} tree — that's what the assembly plugin will pick
 * up when building the distribution zip.
 */
public class IndexOptimizerPackagingTest {

    private Path resourceFile(String relative) {
        // The test executes from the herddb-services module root.
        return Paths.get("src", "main", "resources").resolve(relative);
    }

    @Test
    public void serviceScriptHasIndexOptimizerCase() throws Exception {
        Path serviceScript = resourceFile("bin/service");
        assertTrue("expected " + serviceScript + " to exist", Files.isRegularFile(serviceScript));
        String content = new String(Files.readAllBytes(serviceScript), StandardCharsets.UTF_8);
        assertTrue("bin/service must contain a case for `index-optimizer`",
                content.contains("index-optimizer)"));
        assertTrue("bin/service must wire the optimizer main class",
                content.contains("herddb.indexing.optimizer.IndexOptimizerMain"));
        assertTrue("bin/service usage line must mention index-optimizer",
                content.contains("|index-optimizer"));
    }

    @Test
    public void defaultIndexOptimizerPropertiesExistsAndIsParseable() throws Exception {
        Path propsFile = resourceFile("conf/indexoptimizer.properties");
        assertTrue("expected " + propsFile + " to exist", Files.isRegularFile(propsFile));
        Properties props = new Properties();
        try (java.io.InputStream in = Files.newInputStream(propsFile)) {
            props.load(in);
        }
        // Spot-check that the keys the optimizer needs at startup are set.
        assertNotNull("zookeeper address must be defined by default",
                props.getProperty("indexoptimizer.zookeeper.address"));
        assertNotNull("zookeeper path must be defined by default",
                props.getProperty("indexoptimizer.zookeeper.path"));
        assertNotNull("interval must be defined by default",
                props.getProperty("indexoptimizer.interval.ms"));
        assertNotNull("retention must be defined by default",
                props.getProperty("indexoptimizer.retention.ms"));
    }

    @Test
    public void indexingServicePropertiesStaysCompatible() throws Exception {
        // Sanity check: the existing IS properties file is untouched and still parseable.
        Path propsFile = resourceFile("conf/indexingservice.properties");
        assertTrue("indexingservice.properties must still ship", Files.isRegularFile(propsFile));
        Properties props = new Properties();
        try (java.io.InputStream in = Files.newInputStream(propsFile)) {
            props.load(in);
        }
        assertNotNull(props.getProperty("port"));
    }
}
