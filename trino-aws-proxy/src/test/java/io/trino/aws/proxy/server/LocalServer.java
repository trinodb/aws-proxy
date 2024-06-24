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

import com.google.inject.Key;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credentials;

public final class LocalServer
{
    private static final Logger log = Logger.get(LocalServer.class);

    private LocalServer() {}

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        TestingTrinoS3ProxyServer trinoS3ProxyServer = TestingTrinoS3ProxyServer.builder()
                .withS3Container()
                .withPostgresContainer()
                .withMetastoreContainer()
                .withPySparkContainer()
                .buildAndStart();

        log.info("======== TESTING SERVER STARTED ========");

        TestingHttpServer httpServer = trinoS3ProxyServer.getInjector().getInstance(TestingHttpServer.class);
        Credentials testingCredentials = trinoS3ProxyServer.getInjector().getInstance(Key.get(Credentials.class, ForTesting.class));
        TrinoS3ProxyConfig s3ProxyConfig = trinoS3ProxyServer.getInjector().getInstance(TrinoS3ProxyConfig.class);

        log.info("");
        log.info("Endpoint:   %s", httpServer.getBaseUrl().resolve(s3ProxyConfig.getS3Path()));
        log.info("Access Key: %s", testingCredentials.emulated().accessKey());
        log.info("Secret Key: %s", testingCredentials.emulated().secretKey());
        log.info("");
    }
}
