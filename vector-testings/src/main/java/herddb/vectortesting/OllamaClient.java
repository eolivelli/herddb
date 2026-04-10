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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP client for the Ollama embedding API ({@code POST /api/embed}).
 *
 * <p>Thread-safe: the underlying {@link HttpClient} and {@link JsonFactory} are both safe
 * to share across threads, so a single {@code OllamaClient} can be reused by multiple
 * worker threads performing concurrent {@link #embed(List)} calls.
 */
public class OllamaClient implements Closeable {

    private final HttpClient httpClient;
    private final String baseUrl;
    private final String model;
    private final JsonFactory jsonFactory = new JsonFactory();

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
        byte[] requestBody = buildRequestBody(sentences);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/embed"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(300))
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                .build();

        HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            String snippet = readErrorSnippet(response.body());
            throw new IOException("Ollama API returned HTTP " + response.statusCode() + ": " + snippet);
        }

        try (InputStream body = response.body()) {
            return parseEmbeddings(body);
        }
    }

    private byte[] buildRequestBody(List<String> sentences) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(256 + sentences.size() * 64);
        try (JsonGenerator gen = jsonFactory.createGenerator(baos)) {
            gen.writeStartObject();
            gen.writeStringField("model", model);
            gen.writeArrayFieldStart("input");
            for (String s : sentences) {
                gen.writeString(s);
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }
        return baos.toByteArray();
    }

    private float[][] parseEmbeddings(InputStream body) throws IOException {
        try (JsonParser p = jsonFactory.createParser(body)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Unexpected Ollama response: not a JSON object");
            }
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String field = p.currentName();
                p.nextToken();
                if ("embeddings".equals(field)) {
                    if (p.currentToken() != JsonToken.START_ARRAY) {
                        throw new IOException("Unexpected Ollama response: 'embeddings' is not an array");
                    }
                    List<float[]> rows = new ArrayList<>();
                    while (p.nextToken() == JsonToken.START_ARRAY) {
                        rows.add(readFloatArray(p));
                    }
                    // skip the rest of the object (e.g. "model", "total_duration", ...)
                    while (p.nextToken() != null && p.currentToken() != JsonToken.END_OBJECT) {
                        p.skipChildren();
                    }
                    return rows.toArray(new float[0][]);
                } else {
                    p.skipChildren();
                }
            }
            throw new IOException("Unexpected Ollama response: missing 'embeddings' field");
        }
    }

    private static float[] readFloatArray(JsonParser p) throws IOException {
        // Grow on demand; typical embedding dims are 128..4096.
        float[] buf = new float[512];
        int n = 0;
        while (p.nextToken() != JsonToken.END_ARRAY) {
            if (n == buf.length) {
                float[] bigger = new float[buf.length * 2];
                System.arraycopy(buf, 0, bigger, 0, n);
                buf = bigger;
            }
            buf[n++] = p.getFloatValue();
        }
        if (n == buf.length) {
            return buf;
        }
        float[] out = new float[n];
        System.arraycopy(buf, 0, out, 0, n);
        return out;
    }

    private static String readErrorSnippet(InputStream body) {
        try (InputStream in = body) {
            byte[] buf = new byte[500];
            int total = 0;
            int r;
            while (total < buf.length && (r = in.read(buf, total, buf.length - total)) > 0) {
                total += r;
            }
            return new String(buf, 0, total, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "<unreadable body: " + e.getMessage() + ">";
        }
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
