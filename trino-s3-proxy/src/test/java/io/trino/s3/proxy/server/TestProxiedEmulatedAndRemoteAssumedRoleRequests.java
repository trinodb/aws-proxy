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

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.s3.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServerModule.ForTestingRemoteCredentials;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.s3.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.s3.proxy.spi.credentials.Credentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

public class TestProxiedEmulatedAndRemoteAssumedRoleRequests
        extends TestProxiedAssumedRoleRequests
{
    @Inject
    public TestProxiedEmulatedAndRemoteAssumedRoleRequests(
            TestingHttpServer httpServer,
            @ForTesting Credentials testingCredentials,
            TestingCredentialsRolesProvider credentialsController,
            @ForS3Container S3Client storageClient,
            @ForS3Container List<String> configuredBuckets,
            @ForTestingRemoteCredentials Credentials remoteCredentials,
            TrinoS3ProxyConfig trinoS3ProxyConfig)
    {
        super(buildClient(httpServer, remoteCredentials, trinoS3ProxyConfig.getS3Path(), trinoS3ProxyConfig.getStsPath()), testingCredentials, credentialsController, storageClient, configuredBuckets);
    }
}
