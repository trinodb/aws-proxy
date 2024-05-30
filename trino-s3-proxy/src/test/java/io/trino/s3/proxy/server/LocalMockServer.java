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

import com.google.inject.Key;
import io.airlift.log.Logger;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.testing.TestingConstants.ForTestingCredentials;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;

import java.util.Optional;

import static io.trino.s3.proxy.server.LocalServer.logEndpoint;

public final class LocalMockServer
{
    private static final Logger log = Logger.get(LocalServer.class);

    private LocalMockServer() {}

    public static void main(String[] args)
    {
        TestingTrinoS3ProxyServer trinoS3ProxyServer = TestingTrinoS3ProxyServer.builder()
                .addMockContainer(Optional.empty())
                .buildAndStart();

        log.info("======== TESTING MOCK SERVER STARTED ========");

        logEndpoint(trinoS3ProxyServer);

        Credentials credentials = trinoS3ProxyServer.getInjector().getInstance(Key.get(Credentials.class, ForTestingCredentials.class));

        log.info("Access Key: %s", credentials.emulated().accessKey());
        log.info("Secret Key: %s", credentials.emulated().secretKey());
        log.info("");
    }
}
