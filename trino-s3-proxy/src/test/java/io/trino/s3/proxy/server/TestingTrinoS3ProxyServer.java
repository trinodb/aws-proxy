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
package io.trino.s3.proxy.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.Credentials.Credential;

public final class TestingTrinoS3ProxyServer
{
    private static final Logger log = Logger.get(TestingTrinoS3ProxyServer.class);

    private TestingTrinoS3ProxyServer() {}

    public static void main(String[] args)
    {
        if (args.length != 4) {
            System.err.println("Usage: TestingTrinoS3ProxyServer <emulatedAccessKey> <emulatedSecretKey> <realAccessKey> <realSecretKey>");
            System.exit(1);
        }

        String emulatedAccessKey = args[0];
        String emulatedSecretKey = args[1];
        String realAccessKey = args[2];
        String realSecretKey = args[3];

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingTrinoS3ProxyServerModule())
                .add(new TestingNodeModule())
                .add(new EventModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector = app.initialize();

        TestingCredentialsController credentialsController = injector.getInstance(TestingCredentialsController.class);
        credentialsController.addCredentials(new Credentials(new Credential(emulatedAccessKey, emulatedSecretKey), new Credential(realAccessKey, realSecretKey)));

        log.info("======== TESTING SERVER STARTED ========");

        TestingHttpServer httpServer = injector.getInstance(TestingHttpServer.class);
        log.info("");
        log.info("Endpoint: %s", httpServer.getBaseUrl());
        log.info("");
    }
}
