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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

/** Parsing of the {@code --protocol} / {@code --grpc-endpoint} options. */
class ConfigProtocolTest {

    @Test
    void defaultsToJdbc() throws Exception {
        Config cfg = Config.parse(new String[]{});
        assertEquals(Config.Protocol.JDBC, cfg.protocol);
        assertEquals("localhost:9850", cfg.grpcEndpoint);
    }

    @Test
    void grpcProtocolAndEndpointAreParsed() throws Exception {
        Config cfg = Config.parse(new String[]{
                "--protocol", "grpc", "--grpc-endpoint", "indexer-host:9999"});
        assertEquals(Config.Protocol.GRPC, cfg.protocol);
        assertEquals("indexer-host:9999", cfg.grpcEndpoint);
    }

    @Test
    void protocolIsCaseInsensitive() throws Exception {
        assertEquals(Config.Protocol.GRPC,
                Config.parse(new String[]{"--protocol", "GRPC"}).protocol);
        assertEquals(Config.Protocol.JDBC,
                Config.parse(new String[]{"--protocol", "Jdbc"}).protocol);
    }

    @Test
    void unknownProtocolIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Config.parse(new String[]{"--protocol", "thrift"}));
    }

    @Test
    void protocolCanBeSetFromAPropertiesFile() throws Exception {
        java.io.File props = java.io.File.createTempFile("vectorbench", ".properties");
        props.deleteOnExit();
        try (java.io.FileWriter w = new java.io.FileWriter(props)) {
            w.write("protocol=grpc\n");
            w.write("grpc-endpoint=cfg-host:7777\n");
        }
        Config cfg = Config.parse(new String[]{"--config", props.getAbsolutePath()});
        assertEquals(Config.Protocol.GRPC, cfg.protocol);
        assertEquals("cfg-host:7777", cfg.grpcEndpoint);
    }
}
