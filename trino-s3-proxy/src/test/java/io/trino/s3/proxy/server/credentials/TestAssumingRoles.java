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
package io.trino.s3.proxy.server.credentials;

import com.google.inject.Inject;
import io.trino.s3.proxy.server.testing.TestingConstants.ForTesting;
import io.trino.s3.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.s3.proxy.server.testing.TestingS3ClientProvider;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoS3ProxyTest
public class TestAssumingRoles
{
    private static final String ARN = "test-arn";

    private final TestingCredentialsRolesProvider credentialsController;
    private final TestingS3ClientProvider s3ClientProvider;
    private final Credentials testingCredentials;

    @Inject
    public TestAssumingRoles(TestingCredentialsRolesProvider credentialsController, TestingS3ClientProvider s3ClientProvider, @ForTesting Credentials testingCredentials)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
        this.s3ClientProvider = requireNonNull(s3ClientProvider, "s3ClientProvider is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
    }

    @AfterEach
    public void reset()
    {
        credentialsController.resetAssumedRoles();
    }

    @Test
    public void testStsSession()
    {
        SigningMetadata mockedSigningMetadata = new SigningMetadata(SigningServiceType.S3, testingCredentials, Optional.empty(), "us-east-1");
        EmulatedAssumedRole emulatedAssumedRole = credentialsController.assumeEmulatedRole(mockedSigningMetadata, ARN, Optional.empty(), Optional.empty(), Optional.empty())
                .orElseThrow(() -> new RuntimeException("Failed to assume role"));

        try (S3Client client = s3ClientProvider.newClientBuilder()
                .credentialsProvider(() -> AwsSessionCredentials.create(emulatedAssumedRole.credential().accessKey(), emulatedAssumedRole.credential().secretKey(), emulatedAssumedRole.session()))
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
