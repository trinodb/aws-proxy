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

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServerModule.ForTestingRemoteCredentials;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.spi.credentials.Credentials;
import software.amazon.awssdk.services.s3.S3Client;

public class TestProxiedEmulatedAndRemoteAssumedRoleRequests
        extends TestProxiedAssumedRoleRequests
{
    @Inject
    public TestProxiedEmulatedAndRemoteAssumedRoleRequests(
            TestingHttpServer httpServer,
            TestingCredentialsRolesProvider credentialsController,
            @ForS3Container S3Client storageClient,
            @ForTestingRemoteCredentials Credentials remoteCredentials,
            TrinoAwsProxyConfig trinoAwsProxyConfig,
            TestingS3RequestRewriteController requestRewriteController)
    {
        super(buildClient(httpServer, remoteCredentials, trinoAwsProxyConfig.getS3Path(), trinoAwsProxyConfig.getStsPath()), credentialsController, storageClient, requestRewriteController);
    }
}
