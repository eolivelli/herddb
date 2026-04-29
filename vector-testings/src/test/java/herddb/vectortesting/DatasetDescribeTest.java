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

import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.vectortesting.DatasetLoader.DatasetDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetDescribeTest {

    @Test
    void prettyPrintsAllKeyFieldsAndCheckpoints(@TempDir Path tmp) throws Exception {
        String json = "{\n"
                + "  \"name\": \"smoke\",\n"
                + "  \"format\": \"fvecs\",\n"
                + "  \"dimensions\": 64,\n"
                + "  \"similarity\": \"cosine\",\n"
                + "  \"totalVectors\": 5000,\n"
                + "  \"numQueries\": 100,\n"
                + "  \"groundTruthK\": 10,\n"
                + "  \"baseFile\": \"smoke_base.fvecs\",\n"
                + "  \"queryFile\": \"smoke_query.fvecs\",\n"
                + "  \"groundTruthFile\": \"smoke_groundtruth.ivecs\",\n"
                + "  \"groundTruthCheckpoints\": [\n"
                + "    {\"baseVectorCount\": 1000, \"file\": \"smoke_groundtruth_1000.ivecs\"},\n"
                + "    {\"baseVectorCount\": 5000, \"file\": \"smoke_groundtruth.ivecs\"}\n"
                + "  ]\n"
                + "}\n";
        File descriptorFile = tmp.resolve("smoke_descriptor.json").toFile();
        Files.writeString(descriptorFile.toPath(), json);

        DatasetDescriptor desc = DatasetDescriptor.load(descriptorFile);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DatasetDescribe.print(new PrintStream(buf, true, StandardCharsets.UTF_8), desc);

        String output = buf.toString(StandardCharsets.UTF_8);
        // Spot-check each significant field appears verbatim. The fixed-width formatting
        // is intentionally agent-greppable, so any drift here is a contract violation.
        assertTrue(output.contains("name"), output);
        assertTrue(output.contains("smoke"), output);
        assertTrue(output.contains("dimensions"), output);
        assertTrue(output.contains("64"), output);
        assertTrue(output.contains("similarity"), output);
        assertTrue(output.contains("cosine"), output);
        assertTrue(output.contains("format"), output);
        assertTrue(output.contains("fvecs"), output);
        assertTrue(output.contains("totalVectors"), output);
        assertTrue(output.contains("5000"), output);
        assertTrue(output.contains("groundTruthCheckpoints"), output);
        assertTrue(output.contains("smoke_groundtruth_1000.ivecs"), output);
        assertTrue(output.contains("smoke_groundtruth.ivecs"), output);
    }

    @Test
    void resolveDescriptorReturnsLocalFileVerbatim(@TempDir Path tmp) throws Exception {
        File descriptorFile = tmp.resolve("local_descriptor.json").toFile();
        Files.writeString(descriptorFile.toPath(), "{}");
        File resolved = DatasetDescribe.resolveDescriptor(descriptorFile.getAbsolutePath());
        assertTrue(resolved.exists());
        assertTrue(resolved.getAbsolutePath().endsWith("local_descriptor.json"));
    }

    @Test
    void resolveDescriptorStripsFileScheme(@TempDir Path tmp) throws Exception {
        File descriptorFile = tmp.resolve("schemed_descriptor.json").toFile();
        Files.writeString(descriptorFile.toPath(), "{}");
        File resolved = DatasetDescribe.resolveDescriptor("file://" + descriptorFile.getAbsolutePath());
        assertTrue(resolved.exists());
    }
}
