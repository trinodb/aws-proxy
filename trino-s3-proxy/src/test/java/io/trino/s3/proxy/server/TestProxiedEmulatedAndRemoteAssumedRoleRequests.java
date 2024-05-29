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
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import io.trino.s3.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServerModule.ForTestingRemoteCredentials;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
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
            @ForS3MockContainer S3Client storageClient,
            @ForS3MockContainer List<String> configuredBuckets,
            @ForTestingRemoteCredentials Credentials remoteCredentials)
    {
        super(buildClient(httpServer, remoteCredentials), testingCredentials, credentialsController, storageClient, configuredBuckets);
    }
}
