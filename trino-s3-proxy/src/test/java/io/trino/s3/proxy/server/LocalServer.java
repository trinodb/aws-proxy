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
import io.trino.s3.proxy.server.rest.TrinoS3ProxyRestConstants;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;

public final class LocalServer
{
    private static final Logger log = Logger.get(LocalServer.class);

    private static final String ASSUMED_ROLE_ARN = "arn:aws:iam::123456789012:role/testing";

    private LocalServer() {}

    @SuppressWarnings("resource")
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
        Credentials credentials = new Credentials(new Credential(emulatedAccessKey, emulatedSecretKey), new Credential(realAccessKey, realSecretKey));

        TestingTrinoS3ProxyServer trinoS3ProxyServer = TestingTrinoS3ProxyServer.builder()
                .addCredentials(credentials)
                .buildAndStart();

        log.info("======== TESTING SERVER STARTED ========");

        TestingHttpServer httpServer = trinoS3ProxyServer.getInjector().getInstance(TestingHttpServer.class);
        log.info("");
        log.info("S3 Endpoint: %s", httpServer.getBaseUrl().resolve(TrinoS3ProxyRestConstants.S3_PATH));
        log.info("STS Endpoint: %s", httpServer.getBaseUrl().resolve(TrinoS3ProxyRestConstants.STS_PATH));
        log.info("");
        log.info("Assume Role ARN: %s", ASSUMED_ROLE_ARN);
        log.info("");
    }
}
