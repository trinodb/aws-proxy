/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.aws.proxy.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

public final class TrinoAwsProxyServer
{
    private static final Logger log = Logger.get(TrinoAwsProxyServer.class);
    private final Injector injector;

    private TrinoAwsProxyServer(Injector injector)
    {
        this.injector = requireNonNull(injector, "injector is null");
    }

    public Injector injector()
    {
        return injector;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder() {}

        private final ImmutableSet.Builder<Module> modules = ImmutableSet.builder();

        public Builder addModule(Module module)
        {
            modules.add(module);
            return this;
        }

        public TrinoAwsProxyServer buildAndStart()
        {
            return start(modules.build());
        }
    }

    public static void main(String[] args)
    {
        TrinoAwsProxyServer.builder().buildAndStart();
    }

    private static TrinoAwsProxyServer start(Collection<Module> extraModules)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TrinoAwsProxyServerModule())
                .add(new NodeModule())
                .add(new EventModule())
                .add(new HttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        extraModules.forEach(modules::add);

        try {
            Bootstrap app = new Bootstrap(modules.build());
            Injector injector = app.initialize();

            log.info("======== SERVER STARTED ========");

            return new TrinoAwsProxyServer(injector);
        }
        catch (ApplicationConfigurationException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }

        // will never get here
        return null;
    }
}
