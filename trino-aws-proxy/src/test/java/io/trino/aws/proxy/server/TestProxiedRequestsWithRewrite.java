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
import io.trino.aws.proxy.server.testing.RequestRewriteUtil;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Optional;

import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.CREDENTIAL_TO_REDIRECT;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_BUCKET;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_KEY;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.assertFileNotInS3;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, RequestRewriteUtil.Filter.class})
public class TestProxiedRequestsWithRewrite
        extends AbstractTestProxiedRequests
{
    private final URI baseUri;
    private final String relativePath;

    @Inject
    public TestProxiedRequestsWithRewrite(
            S3Client s3Client,
            @ForS3Container S3Client storageClient,
            TestingS3RequestRewriteController s3RequestRewriteController,
            TestingHttpServer testingHttpServer,
            TrinoAwsProxyConfig config)
    {
        super(s3Client, storageClient, s3RequestRewriteController);
        this.baseUri = testingHttpServer.getBaseUrl();
        this.relativePath = config.getS3Path();
    }

    @Test
    public void testRewriteBasedOnIdentity()
            throws IOException
    {
        String testBucket = "dummy";
        String testKey = "dummy-key";
        try (S3Client testS3Client = clientBuilder(baseUri, Optional.of(relativePath))
                .credentialsProvider(() -> AwsBasicCredentials.create(CREDENTIAL_TO_REDIRECT.accessKey(), CREDENTIAL_TO_REDIRECT.secretKey()))
                .build()) {
            PutObjectRequest uploadRequest = PutObjectRequest.builder().bucket(testBucket).key(testKey).build();
            PutObjectResponse uploadResponse = testS3Client.putObject(uploadRequest, TEST_FILE);
            assertThat(uploadResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

            assertThat(getFileFromStorage(testS3Client, testBucket, testKey)).isEqualTo(Files.readString(TEST_FILE));
        }
        assertThat(getFileFromStorage(remoteClient, TEST_CREDENTIAL_REDIRECT_BUCKET, TEST_CREDENTIAL_REDIRECT_KEY)).isEqualTo(Files.readString(TEST_FILE));
        assertFileNotInS3(remoteClient, testBucket, testKey);
    }
}
