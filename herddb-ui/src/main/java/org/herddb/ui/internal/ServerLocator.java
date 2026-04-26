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

package org.herddb.ui.internal;

import herddb.server.Server;
import javax.servlet.ServletContext;

/**
 * Looks up the running {@link Server} associated with a webapp. The Server is
 * bound by {@code ServerMain} as a {@link ServletContext} attribute under the
 * key {@link #SERVLET_ATTRIBUTE} when the embedded Jetty starts; tests bind
 * the same attribute on a stub {@link ServletContext} (or call
 * {@link #of(Server)} to wrap an existing {@code Server}).
 *
 * <p>Using a {@link ServletContext} attribute (rather than a static singleton)
 * avoids global state, so multiple {@code Server} instances can coexist in
 * the same JVM during tests.
 */
public final class ServerLocator {

    /**
     * {@link ServletContext} attribute key under which the running
     * {@link Server} is bound by the embedded Jetty.
     */
    public static final String SERVLET_ATTRIBUTE = "herddb.server";

    private final Server server;

    private ServerLocator(Server server) {
        if (server == null) {
            throw new IllegalArgumentException("server cannot be null");
        }
        this.server = server;
    }

    /**
     * Wraps an existing {@link Server} as a {@code ServerLocator}. Intended
     * for unit tests that construct a {@code Server} directly and pass it to
     * a JAX-RS resource without going through the Servlet API.
     *
     * @param server a running {@link Server}; must not be {@code null}
     * @return a {@code ServerLocator} backed by the given server
     */
    public static ServerLocator of(Server server) {
        return new ServerLocator(server);
    }

    /**
     * Resolves the {@link ServerLocator} (and therefore the running
     * {@link Server}) bound to the given {@link ServletContext}.
     *
     * @param servletContext the JAX-RS injected ServletContext; must not be
     *                       {@code null}
     * @return the bound locator
     * @throws IllegalStateException if no Server is bound yet
     */
    public static ServerLocator fromServletContext(ServletContext servletContext) {
        if (servletContext == null) {
            throw new IllegalArgumentException("servletContext cannot be null");
        }
        Object attribute = servletContext.getAttribute(SERVLET_ATTRIBUTE);
        if (attribute instanceof ServerLocator) {
            return (ServerLocator) attribute;
        }
        if (attribute instanceof Server) {
            // Tolerate the simpler "raw Server" shape — useful for tests that
            // bind a Server directly without wrapping.
            return ServerLocator.of((Server) attribute);
        }
        throw new IllegalStateException(
                "HerdDB Server not bound to ServletContext under attribute '"
                        + SERVLET_ATTRIBUTE + "'");
    }

    /**
     * @return the running {@link Server}; never {@code null}
     */
    public Server getServer() {
        return server;
    }
}
