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
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyRestConstants;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.s3.proxy.server.testing.harness.BuilderFilter;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@TrinoS3ProxyTest(initialBuckets = "one,two,three", filters = TestProxiedAssumedRoleRequests.Filter.class)
public class TestProxiedAssumedRoleRequests
        extends AbstractTestProxiedRequests
{
    private static final String ARN = "arn:aws:iam::123456789012:role/assumed";

    private static final Credentials credentials = new Credentials(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            Credential assumedCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            return builder
                    .addCredentials(credentials)
                    .addAssumedRole(credentials.emulated().accessKey(), ARN, new Credentials(assumedCredential, credentials.real()));
        }
    }

    @Inject
    public TestProxiedAssumedRoleRequests(TestingHttpServer httpServer)
    {
        super(buildClient(httpServer));
    }

    private static S3Client buildClient(TestingHttpServer httpServer)
    {
        URI baseUrl = httpServer.getBaseUrl();
        URI localProxyServerUri = baseUrl.resolve(TrinoS3ProxyRestConstants.S3_PATH);
        URI localStsServerUri = baseUrl.resolve(TrinoS3ProxyRestConstants.STS_PATH);

        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(credentials.emulated().accessKey(), credentials.emulated().secretKey());

        StsClient stsClient = StsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .endpointProvider(endpointParams -> CompletableFuture.completedFuture(Endpoint.builder().url(localStsServerUri).build()))
                .build();

        StsAssumeRoleCredentialsProvider credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(ARN)
                        .roleSessionName("dummy")
                        .externalId("dummy"))
                .stsClient(stsClient)
                .asyncCredentialUpdateEnabled(true)
                .build();

        return S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(localProxyServerUri)
                .build();
    }
}
