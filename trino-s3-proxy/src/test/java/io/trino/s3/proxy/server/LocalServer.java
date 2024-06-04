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

import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;

public final class LocalServer
{
    private static final Logger log = Logger.get(LocalServer.class);

    private LocalServer() {}

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        if (args.length != 4) {
            System.err.println("Usage: TestingTrinoS3ProxyServer <emulatedAccessKey> <emulatedSecretKey> <remoteAccessKey> <remoteSecretKey>");
            System.exit(1);
        }

        String emulatedAccessKey = args[0];
        String emulatedSecretKey = args[1];
        String remoteAccessKey = args[2];
        String remoteSecretKey = args[3];
        Credentials credentials = Credentials.build(new Credential(emulatedAccessKey, emulatedSecretKey), new Credential(remoteAccessKey, remoteSecretKey));

        TestingTrinoS3ProxyServer trinoS3ProxyServer = TestingTrinoS3ProxyServer.builder()
                .withMockS3Container()
                .buildAndStart();
        trinoS3ProxyServer.getCredentialsController().addCredentials(credentials);

        log.info("======== TESTING SERVER STARTED ========");

        TestingHttpServer httpServer = trinoS3ProxyServer.getInjector().getInstance(TestingHttpServer.class);
        log.info("");
        log.info("Endpoint: %s", httpServer.getBaseUrl());
        log.info("");
    }
}
