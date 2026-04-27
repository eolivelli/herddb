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

package org.herddb.ui.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link SpaFallbackFilter}.
 *
 * <p>Each test builds a minimal stub {@link HttpServletRequest} via
 * {@link java.lang.reflect.Proxy} — only the three methods the filter
 * actually invokes ({@code getRequestURI}, {@code getContextPath},
 * {@code getMethod}, and {@code getRequestDispatcher}) are implemented.
 * All other interface methods throw {@link UnsupportedOperationException}
 * so that any accidental call surfaces immediately.
 */
public class SpaFallbackFilterTest {

    private SpaFallbackFilter filter;

    @Before
    public void setUp() throws ServletException {
        filter = new SpaFallbackFilter();
        filter.init(null);
    }

    // ------------------------------------------------------------------
    // Helper: build a stub HttpServletRequest via a dynamic proxy
    // ------------------------------------------------------------------

    /**
     * Returns a proxy {@link HttpServletRequest} that answers the four
     * methods used by the filter.  All other method calls throw
     * {@link UnsupportedOperationException}.
     *
     * @param requestUri   value returned by {@code getRequestURI()}
     * @param contextPath  value returned by {@code getContextPath()}
     * @param method       value returned by {@code getMethod()}
     * @param dispatcher   value returned by {@code getRequestDispatcher(…)}
     *                     (may be {@code null} if the filter is not expected
     *                     to call that method in the tested scenario)
     */
    private static HttpServletRequest stubRequest(
            String requestUri,
            String contextPath,
            String method,
            RequestDispatcher dispatcher) {

        return (HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                SpaFallbackFilterTest.class.getClassLoader(),
                new Class<?>[]{HttpServletRequest.class},
                (proxy, m, args) -> {
                    switch (m.getName()) {
                        case "getRequestURI":
                            return requestUri;
                        case "getContextPath":
                            return contextPath;
                        case "getMethod":
                            return method;
                        case "getRequestDispatcher":
                            return dispatcher;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unexpected call in test: " + m.getName());
                    }
                });
    }

    /** Records whether {@link FilterChain#doFilter} was invoked. */
    private static FilterChain recordingChain(AtomicBoolean called) {
        return (req, res) -> called.set(true);
    }

    /** Records the path passed to {@link RequestDispatcher#forward}. */
    private static RequestDispatcher recordingDispatcher(
            AtomicBoolean forwardCalled,
            AtomicReference<String> capturedPath,
            String path) {

        return new RequestDispatcher() {
            @Override
            public void forward(ServletRequest req, ServletResponse res)
                    throws ServletException, IOException {
                forwardCalled.set(true);
                capturedPath.set(path);
            }

            @Override
            public void include(ServletRequest req, ServletResponse res) {
                throw new UnsupportedOperationException("include not expected");
            }
        };
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /**
     * Requests whose path starts with {@code /api/} must be forwarded
     * through the filter chain without touching the request dispatcher.
     */
    @Test
    public void apiRequestPassesThroughChain() throws Exception {
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicBoolean forwardCalled = new AtomicBoolean(false);

        HttpServletRequest request = stubRequest(
                "/api/v2/health", "", "GET",
                recordingDispatcher(forwardCalled, new AtomicReference<>(), "/index.html"));

        filter.doFilter(request, null, recordingChain(chainCalled));

        assertTrue("chain must be called for /api/v2/health", chainCalled.get());
        assertFalse("dispatcher must NOT be called for API paths", forwardCalled.get());
    }

    /**
     * Requests for static assets (last path segment contains {@code '.'})
     * must pass through the chain without a forward to {@code /index.html}.
     */
    @Test
    public void staticAssetRequestPassesThroughChain() throws Exception {
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicBoolean forwardCalled = new AtomicBoolean(false);

        for (String assetPath : new String[]{
                "/assets/main.abc123.js",
                "/assets/theme.css",
                "/favicon.ico",
                "/logo.png"}) {

            chainCalled.set(false);
            forwardCalled.set(false);

            HttpServletRequest request = stubRequest(
                    assetPath, "", "GET",
                    recordingDispatcher(forwardCalled, new AtomicReference<>(), "/index.html"));

            filter.doFilter(request, null, recordingChain(chainCalled));

            assertTrue("chain must be called for " + assetPath, chainCalled.get());
            assertFalse("dispatcher must NOT be called for " + assetPath, forwardCalled.get());
        }
    }

    /**
     * Deep SPA routes (GET requests without a file extension) must be
     * forwarded to {@code /index.html} so React Router can render them.
     */
    @Test
    public void spaRouteIsForwardedToIndexHtml() throws Exception {
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicBoolean forwardCalled = new AtomicBoolean(false);
        AtomicReference<String> capturedPath = new AtomicReference<>();

        for (String spaPath : new String[]{
                "/tablespaces",
                "/tablespaces/herd/tables",
                "/tablespaces/herd/tables/vector_bench/data-pages",
                "/tablespaces/herd/tables/vector_bench/primary-index",
                "/indexing-services"}) {

            chainCalled.set(false);
            forwardCalled.set(false);
            capturedPath.set(null);

            HttpServletRequest request = stubRequest(
                    spaPath, "", "GET",
                    recordingDispatcher(forwardCalled, capturedPath, spaPath));

            filter.doFilter(request, null, recordingChain(chainCalled));

            assertFalse("chain must NOT be called for SPA route " + spaPath, chainCalled.get());
            assertTrue("dispatcher forward must be called for SPA route " + spaPath, forwardCalled.get());
        }
    }

    /**
     * The root path {@code /} is also a SPA route and must be forwarded
     * to {@code /index.html}.
     */
    @Test
    public void rootPathIsForwardedToIndexHtml() throws Exception {
        AtomicBoolean forwardCalled = new AtomicBoolean(false);
        AtomicBoolean chainCalled = new AtomicBoolean(false);

        HttpServletRequest request = stubRequest(
                "/", "", "GET",
                recordingDispatcher(forwardCalled, new AtomicReference<>(), "/"));

        filter.doFilter(request, null, recordingChain(chainCalled));

        assertFalse("chain must NOT be called for /", chainCalled.get());
        assertTrue("dispatcher forward must be called for /", forwardCalled.get());
    }

    /**
     * Non-GET requests on SPA-style paths must pass through the chain
     * unchanged.  Only GET is used for SPA page navigation; rewriting
     * POST/PUT/DELETE would silently corrupt REST calls to unmapped paths.
     */
    @Test
    public void nonGetSpaPathPassesThroughChain() throws Exception {
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicBoolean forwardCalled = new AtomicBoolean(false);

        for (String httpMethod : new String[]{"POST", "PUT", "DELETE", "PATCH"}) {
            chainCalled.set(false);
            forwardCalled.set(false);

            HttpServletRequest request = stubRequest(
                    "/tablespaces/herd/tables", "", httpMethod,
                    recordingDispatcher(forwardCalled, new AtomicReference<>(), "/index.html"));

            filter.doFilter(request, null, recordingChain(chainCalled));

            assertTrue("chain must be called for " + httpMethod, chainCalled.get());
            assertFalse("dispatcher must NOT be called for " + httpMethod, forwardCalled.get());
        }
    }

    /**
     * The filter should strip the context path before inspecting the path.
     * A request with URI {@code /ui/tablespaces} and context path {@code /ui}
     * must be treated as the SPA route {@code /tablespaces}.
     */
    @Test
    public void contextPathIsStrippedBeforeMatching() throws Exception {
        AtomicBoolean forwardCalled = new AtomicBoolean(false);
        AtomicBoolean chainCalled = new AtomicBoolean(false);

        HttpServletRequest request = stubRequest(
                "/ui/tablespaces/herd/tables/foo/data-pages", "/ui", "GET",
                recordingDispatcher(forwardCalled, new AtomicReference<>(),
                        "/tablespaces/herd/tables/foo/data-pages"));

        filter.doFilter(request, null, recordingChain(chainCalled));

        assertFalse("chain must NOT be called for SPA route under /ui context", chainCalled.get());
        assertTrue("dispatcher forward must be called for SPA route under /ui context", forwardCalled.get());
    }

    /**
     * An API request under the context path must still pass through the chain.
     */
    @Test
    public void apiRequestUnderContextPathPassesThroughChain() throws Exception {
        AtomicBoolean chainCalled = new AtomicBoolean(false);
        AtomicBoolean forwardCalled = new AtomicBoolean(false);

        HttpServletRequest request = stubRequest(
                "/ui/api/v2/health", "/ui", "GET",
                recordingDispatcher(forwardCalled, new AtomicReference<>(), "/index.html"));

        filter.doFilter(request, null, recordingChain(chainCalled));

        assertTrue("chain must be called for API path under /ui context", chainCalled.get());
        assertFalse("dispatcher must NOT be called for API path", forwardCalled.get());
    }

    /**
     * Verify the path used when forwarding to the SPA entry point is
     * exactly {@code /index.html}.
     */
    @Test
    public void forwardTargetIsIndexHtml() throws Exception {
        AtomicReference<String> capturedDispatcherPath = new AtomicReference<>();

        // Intercept the getRequestDispatcher call to capture the path argument
        HttpServletRequest request = (HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{HttpServletRequest.class},
                (proxy, m, args) -> {
                    switch (m.getName()) {
                        case "getRequestURI":
                            return "/tablespaces/herd/tables/foo";
                        case "getContextPath":
                            return "";
                        case "getMethod":
                            return "GET";
                        case "getRequestDispatcher":
                            String path = (String) args[0];
                            capturedDispatcherPath.set(path);
                            return new RequestDispatcher() {
                                @Override
                                public void forward(ServletRequest req, ServletResponse res) {
                                    // no-op for this test
                                }
                                @Override
                                public void include(ServletRequest req, ServletResponse res) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        default:
                            throw new UnsupportedOperationException(m.getName());
                    }
                });

        filter.doFilter(request, null, (req, res) -> {
            throw new AssertionError("chain must not be called for SPA route");
        });

        assertEquals("forward must target /index.html", "/index.html", capturedDispatcherPath.get());
    }
}
