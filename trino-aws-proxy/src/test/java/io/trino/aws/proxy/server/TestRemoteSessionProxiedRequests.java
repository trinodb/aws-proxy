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
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection.StaticRemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.UUID;

import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_IDENTITY_CREDENTIAL;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestRemoteSessionProxiedRequests
        extends AbstractTestProxiedRequests
{
    @Inject
    public TestRemoteSessionProxiedRequests(@ForS3Container S3Client storageClient, S3Container s3Container, TestingCredentialsRolesProvider testingCredentialsRolesProvider,
            TestingHttpServer httpServer, TrinoAwsProxyConfig trinoAwsProxyConfig, TestingS3RequestRewriteController requestRewriteController)
    {
        super(buildClient(httpServer, trinoAwsProxyConfig, s3Container, testingCredentialsRolesProvider), storageClient, requestRewriteController);
    }

    private static S3Client buildClient(TestingHttpServer httpServer, TrinoAwsProxyConfig trinoAwsProxyConfig, S3Container s3Container,
            TestingCredentialsRolesProvider testingCredentialsRolesProvider)
    {
        Credential policyUserCredential = s3Container.policyUserCredential();
        RemoteSessionRole remoteSessionRole = new RemoteSessionRole("us-east-1", "minio-doesnt-care", Optional.empty(), Optional.empty());
        IdentityCredential identityCredential = new IdentityCredential(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                TESTING_IDENTITY_CREDENTIAL.identity());
        testingCredentialsRolesProvider.addCredentials(identityCredential, new StaticRemoteS3Connection(policyUserCredential, remoteSessionRole));
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(identityCredential.emulated().accessKey(), identityCredential.emulated().secretKey());
        return clientBuilder(httpServer.getBaseUrl(), Optional.of(trinoAwsProxyConfig.getS3Path()))
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }
}
