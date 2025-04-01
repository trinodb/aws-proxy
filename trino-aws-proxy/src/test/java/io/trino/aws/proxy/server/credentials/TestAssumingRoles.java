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
package io.trino.aws.proxy.server.credentials;

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.EmulatedAssumedRole;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.Optional;

import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoAwsProxyTest
public class TestAssumingRoles
{
    private static final String ARN = "test-arn";

    private final TestingCredentialsRolesProvider credentialsController;
    private final URI localS3URI;
    private final IdentityCredential testingCredentials;

    @Inject
    public TestAssumingRoles(TestingCredentialsRolesProvider credentialsController, TestingHttpServer httpServer, @ForTesting IdentityCredential testingCredentials,
            TrinoAwsProxyConfig trinoS3ProxyConfig)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.localS3URI = httpServer.getBaseUrl().resolve(trinoS3ProxyConfig.getS3Path());
    }

    @AfterEach
    public void reset()
    {
        credentialsController.resetAssumedRoles();
    }

    @Test
    public void testStsSession()
    {
        EmulatedAssumedRole emulatedAssumedRole = credentialsController.assumeEmulatedRole(testingCredentials.emulated(), "us-east-1", ARN, Optional.empty(),
                        Optional.empty(), Optional.empty())
                .orElseThrow(() -> new RuntimeException("Failed to assume role"));

        try (S3Client client = clientBuilder(localS3URI)
                .credentialsProvider(() -> AwsSessionCredentials.create(emulatedAssumedRole.emulatedCredential().accessKey(), emulatedAssumedRole.emulatedCredential().secretKey(), emulatedAssumedRole.emulatedCredential().session().orElseThrow()))
                .build()) {
            // valid assumed role session - should work
            ListBucketsResponse listBucketsResponse = client.listBuckets();
            assertThat(listBucketsResponse.buckets()).isEmpty();

            credentialsController.resetAssumedRoles();

            // invalid assumed role session - should fail
            assertThatThrownBy(client::listBuckets)
                    .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                    .extracting(S3Exception::statusCode)
                    .isEqualTo(401);
        }
    }
}
