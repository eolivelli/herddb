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

package org.herddb.ui.api.v2;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.herddb.ui.internal.ServerLocator;

/**
 * Root JAX-RS application for the Web UI v2 REST surface.
 *
 * <p>All endpoints declared by classes in this package are mounted under
 * {@code /api/v2/...} on the embedded Jetty server started by
 * {@code ServerMain}. The running {@code Server} reaches the resources
 * through HK2 dependency injection: this application registers a
 * {@link Factory} that resolves a {@link ServerLocator} from the
 * {@link ServletContext} attribute populated by {@code ServerMain}
 * (see {@code ServerLocator.SERVLET_ATTRIBUTE}).
 *
 * <p>HTTP smoke tests bypass this binding by registering a different
 * {@link AbstractBinder} that supplies a fixed, test-controlled
 * {@link ServerLocator}.
 */
@ApplicationPath("/api/v2")
public class ApplicationConfigV2 extends ResourceConfig {

    public ApplicationConfigV2() {
        register(JacksonFeature.class);
        register(HealthResource.class);
        register(TablespacesResource.class);
        register(ServerInfoResource.class);
        register(TablesResource.class);
        register(new ServerLocatorBinder());
    }

    /**
     * HK2 binder that exposes the {@link ServerLocator} pulled from the
     * webapp's {@link ServletContext}. Bound as a {@link Singleton} because
     * the locator never changes during the lifetime of the server.
     */
    private static final class ServerLocatorBinder extends AbstractBinder {
        @Override
        protected void configure() {
            bindFactory(ServletContextServerLocatorFactory.class)
                    .to(ServerLocator.class)
                    .in(Singleton.class);
        }
    }

    /**
     * Factory that builds a {@link ServerLocator} on demand by looking up
     * the running {@code Server} in the injected {@link ServletContext}.
     */
    public static final class ServletContextServerLocatorFactory implements Factory<ServerLocator> {

        private final ServletContext servletContext;

        @Inject
        public ServletContextServerLocatorFactory(@Context ServletContext servletContext) {
            this.servletContext = servletContext;
        }

        @Override
        public ServerLocator provide() {
            return ServerLocator.fromServletContext(servletContext);
        }

        @Override
        public void dispose(ServerLocator instance) {
            // no-op — the Server's lifecycle is owned by ServerMain
        }
    }
}
