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

package herddb.remote;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Consistent hash ring using Murmur3. Maps a path to a server address.
 *
 * @author enrico.olivelli
 */
public class ConsistentHashRouter {

    private static final int VIRTUAL_NODES_PER_SERVER = 150;

    @SuppressWarnings("deprecation")
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private final TreeMap<Integer, String> ring = new TreeMap<>();
    private final List<String> servers;

    public ConsistentHashRouter(List<String> servers) {
        if (servers == null) {
            throw new IllegalArgumentException("Server list must not be null");
        }
        this.servers = List.copyOf(servers);
        for (String server : servers) {
            for (int i = 0; i < VIRTUAL_NODES_PER_SERVER; i++) {
                int hash = hash(server + "#" + i);
                ring.put(hash, server);
            }
        }
    }

    /**
     * Returns the server address responsible for the given path.
     */
    public String getServer(String path) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Hash ring is empty");
        }
        int hash = hash(path);
        Map.Entry<Integer, String> entry = ring.ceilingEntry(hash);
        return entry != null ? entry.getValue() : ring.firstEntry().getValue();
    }

    public List<String> getServers() {
        return servers;
    }

    private static int hash(String key) {
        return HASH_FUNCTION.hashString(key, StandardCharsets.UTF_8).asInt();
    }
}
