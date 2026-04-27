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

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

/**
 * SPA (Single-Page Application) fallback filter.
 *
 * <p>When the React front-end uses the HTML5 History API ({@code BrowserRouter})
 * the URL in the browser's address bar changes without any round-trip to the
 * server.  On a hard refresh (Ctrl-R) or a direct deep-link access the browser
 * issues a real HTTP GET for the current path — for example
 * {@code /tablespaces/herd/tables/vector_bench/data-pages}.  Jetty's default
 * servlet cannot find a static file at that path and returns 404.
 *
 * <p>This filter intercepts those requests and forwards them to
 * {@code /index.html} so that React Router can take over and render the
 * correct page client-side.  Three types of request are <em>not</em> forwarded
 * and instead fall through the filter chain unchanged:
 * <ol>
 *   <li>Requests whose path starts with {@code /api/} — handled by the
 *       Jersey JAX-RS servlet.</li>
 *   <li>Requests whose last path segment contains a {@code '.'} — these are
 *       static assets (JavaScript bundles, CSS, images, …) that the default
 *       servlet must serve directly.</li>
 *   <li>Non-GET requests on any path — only GET is used for SPA navigation;
 *       POST/PUT/DELETE on an unmapped path should not be silently rewritten.</li>
 * </ol>
 */
public class SpaFallbackFilter implements Filter {

    private static final String INDEX_HTML = "/index.html";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // no initialisation required
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String path = httpRequest.getRequestURI()
                .substring(httpRequest.getContextPath().length());

        // 1. REST API — let Jersey handle it
        if (path.startsWith("/api/")) {
            chain.doFilter(request, response);
            return;
        }

        // 2. Static assets — any path segment that contains a '.' is assumed
        //    to be a real file (e.g. main.js, theme.css, favicon.ico).
        String lastSegment = path.substring(path.lastIndexOf('/') + 1);
        if (lastSegment.contains(".")) {
            chain.doFilter(request, response);
            return;
        }

        // 3. Non-GET methods on unmapped paths — pass through without rewriting.
        if (!"GET".equalsIgnoreCase(httpRequest.getMethod())) {
            chain.doFilter(request, response);
            return;
        }

        // SPA route — forward to the React entry point.
        httpRequest.getRequestDispatcher(INDEX_HTML).forward(request, response);
    }

    @Override
    public void destroy() {
        // no resources to release
    }
}
