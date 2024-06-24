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

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestStsRequests
{
    private final StsClient stsClient;
    private final CredentialsProvider credentialsProvider;

    public AbstractTestStsRequests(Credentials testingCredentials, TestingHttpServer httpServer, CredentialsProvider credentialsProvider, TrinoS3ProxyConfig s3ProxyConfig)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");

        URI localProxyServerUri = httpServer.getBaseUrl().resolve(s3ProxyConfig.getStsPath());
        Endpoint endpoint = Endpoint.builder().url(localProxyServerUri).build();

        stsClient = StsClient.builder()
                .region(Region.US_EAST_1)
                .endpointProvider(_ -> completedFuture(endpoint))
                .credentialsProvider(() -> AwsBasicCredentials.create(testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey()))
                .build();
    }

    @Test
    public void testAssumeRole()
    {
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder().roleArn("dummy").roleSessionName("dummy").build();
        AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);

        software.amazon.awssdk.services.sts.model.Credentials awsCredentials = assumeRoleResponse.credentials();

        Optional<Credentials> credentials = credentialsProvider.credentials(awsCredentials.accessKeyId(), Optional.of(awsCredentials.sessionToken()));
        assertThat(credentials).isNotEmpty();
        assertThat(credentials.map(Credentials::emulated)).contains(new Credential(awsCredentials.accessKeyId(), awsCredentials.secretAccessKey()));
    }
}
