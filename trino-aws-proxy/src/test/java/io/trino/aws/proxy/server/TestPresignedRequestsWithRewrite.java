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

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.RequestRewriteUtil;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Optional;

import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.CREDENTIAL_TO_REDIRECT;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_BUCKET;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_KEY;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_REWRITTEN_TAGS;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, WithTestingHttpClient.class, RequestRewriteUtil.Filter.class})
public class TestPresignedRequestsWithRewrite
        extends AbstractTestPresignedRequests
{
    private final URI baseUri;
    private final String relativePath;

    @Inject
    public TestPresignedRequestsWithRewrite(
            @ForTesting HttpClient httpClient,
            S3Client internalClient,
            @ForS3Container S3Client storageClient,
            @ForTesting IdentityCredential testingCredentials,
            TestingHttpServer httpServer,
            TrinoAwsProxyConfig s3ProxyConfig,
            XmlMapper xmlMapper,
            TestingS3RequestRewriteController requestRewriteController)
    {
        super(httpClient, internalClient, storageClient, testingCredentials, httpServer, s3ProxyConfig, xmlMapper, requestRewriteController);
        this.baseUri = httpServer.getBaseUrl();
        this.relativePath = s3ProxyConfig.getS3Path();
    }

    @Test
    public void testPresignedRedirectBasedOnIdentity()
            throws IOException
    {
        uploadFileToStorage(TEST_CREDENTIAL_REDIRECT_BUCKET, TEST_CREDENTIAL_REDIRECT_KEY, TEST_FILE);

        try (S3Presigner presigner = buildPresigner(CREDENTIAL_TO_REDIRECT)) {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket("foo")
                    .key("does-not-matter")
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofDays(1))
                    .getObjectRequest(objectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
            StringResponseHandler.StringResponse response = executeHttpRequest(presignedRequest.httpRequest(), createStringResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
            assertThat(response.getBody()).isEqualTo(Files.readString(TEST_FILE));
        }
    }

    @Test
    public void testPresignedUploadRedirectAndModifyHeadersBasedOnIdentity()
            throws IOException
    {
        String testBucket = "two";
        String testKey = "does-not-matter";
        String fileContents = Files.readString(TEST_FILE, StandardCharsets.UTF_8);

        try (
                S3Presigner presigner = buildPresigner(CREDENTIAL_TO_REDIRECT);
                S3Client testS3Client = clientBuilder(baseUri, Optional.of(relativePath))
                        .credentialsProvider(() -> AwsBasicCredentials.create(CREDENTIAL_TO_REDIRECT.accessKey(), CREDENTIAL_TO_REDIRECT.secretKey()))
                        .build()) {
            PutObjectRequest request = PutObjectRequest
                    .builder()
                    .bucket(testBucket)
                    .key(testKey)
                    .contentEncoding("gzip")
                    .contentType("text/plain;charset=UTF-8")
                    .build();
            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofDays(1))
                    .putObjectRequest(request)
                    .build();
            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

            StatusResponseHandler.StatusResponse response = executeHttpRequest(presignedRequest.httpRequest(), fileContents, createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
            assertThat(getFileFromStorage(testS3Client, testBucket, testKey, Optional.of(CREDENTIAL_TO_REDIRECT))).isEqualTo(fileContents);
            assertThat(getObjectTagging(testS3Client, testBucket, testKey, Optional.of(CREDENTIAL_TO_REDIRECT))).isEqualTo(TEST_REWRITTEN_TAGS);
        }
    }
}
