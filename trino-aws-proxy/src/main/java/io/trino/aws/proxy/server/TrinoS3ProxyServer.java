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
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;

public final class TrinoS3ProxyServer
{
    private static final Logger log = Logger.get(TrinoS3ProxyServer.class);

    private TrinoS3ProxyServer() {}

    public static void main(String[] args)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TrinoS3ProxyServerModule())
                .add(new NodeModule())
                .add(new EventModule())
                .add(new HttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector = app.initialize();

        log.info("======== SERVER STARTED ========");
    }
}
