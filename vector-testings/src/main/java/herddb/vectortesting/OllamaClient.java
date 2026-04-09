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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * HTTP client for the Ollama embedding API ({@code POST /api/embed}).
 */
public class OllamaClient implements Closeable {

    private final HttpClient httpClient;
    private final String baseUrl;
    private final String model;
    private final ObjectMapper mapper = new ObjectMapper();

    public OllamaClient(String baseUrl, String model) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * Embed a batch of sentences. Returns one float[] per input sentence.
     */
    public float[][] embed(List<String> sentences) throws IOException, InterruptedException {
        ObjectNode requestBody = mapper.createObjectNode();
        requestBody.put("model", model);
        ArrayNode inputArray = requestBody.putArray("input");
        for (String s : sentences) {
            inputArray.add(s);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/embed"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(300))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(requestBody)))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Ollama API returned HTTP " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        JsonNode embeddings = root.get("embeddings");
        if (embeddings == null || !embeddings.isArray()) {
            throw new IOException("Unexpected Ollama response: missing 'embeddings' array. Response: "
                    + response.body().substring(0, Math.min(500, response.body().length())));
        }

        float[][] result = new float[embeddings.size()][];
        for (int i = 0; i < embeddings.size(); i++) {
            JsonNode vec = embeddings.get(i);
            float[] floats = new float[vec.size()];
            for (int j = 0; j < vec.size(); j++) {
                floats[j] = (float) vec.get(j).doubleValue();
            }
            result[i] = floats;
        }
        return result;
    }

    /**
     * Probes the embedding dimension by embedding a test sentence.
     */
    public int probeDimension() throws IOException, InterruptedException {
        float[][] result = embed(List.of("test"));
        return result[0].length;
    }

    @Override
    public void close() {
        // HttpClient does not require explicit close in Java 21
    }
}
