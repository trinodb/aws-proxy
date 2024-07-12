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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
import io.trino.aws.proxy.spi.credentials.Credentials;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.CompleteMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.CreateMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.DeleteObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedDeleteObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedUploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, WithTestingHttpClient.class, TestProxiedRequests.Filter.class})
public class TestPresignedRequests
{
    private final HttpClient httpClient;
    private final S3Client internalClient;
    private final S3Client storageClient;
    private final Credentials testingCredentials;
    private final URI s3ProxyUrl;
    private final XmlMapper xmlMapper;

    private static final Duration TEST_SIGNATURE_DURATION = Duration.ofMinutes(10);

    @Inject
    public TestPresignedRequests(
            @ForTesting HttpClient httpClient,
            S3Client internalClient,
            @ForS3Container S3Client storageClient,
            @ForTesting Credentials testingCredentials,
            TestingHttpServer httpServer,
            TrinoS3ProxyConfig s3ProxyConfig,
            XmlMapper xmlMapper)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.s3ProxyUrl = httpServer.getBaseUrl().resolve(s3ProxyConfig.getS3Path());
        this.xmlMapper = requireNonNull(xmlMapper, "xmlMapper is null");
    }

    @Test
    public void testPresignedGet()
            throws URISyntaxException, IOException
    {
        uploadFileToStorage("one", "presignedGet", TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket("one")
                    .key("presignedGet")
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .getObjectRequest(objectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
            Request request = prepareGet()
                    .setUri(presignedRequest.url().toURI())
                    .build();
            StringResponse response = httpClient.execute(request, createStringResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
            assertThat(response.getBody()).isEqualTo(Files.readString(TEST_FILE));
        }
    }

    @Test
    public void testPresignedPut()
            throws URISyntaxException, IOException
    {
        String fileContents = Files.readString(TEST_FILE, StandardCharsets.UTF_8);

        try (S3Presigner presigner = buildPresigner()) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket("two").key("presignedPut").build();
            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .putObjectRequest(putObjectRequest)
                    .build();
            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

            Request request = preparePut()
                    .setUri(presignedRequest.url().toURI())
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(fileContents, StandardCharsets.UTF_8))
                    .build();
            StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
        }

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("two").key("presignedPut").build();
        ByteArrayOutputStream readContents = new ByteArrayOutputStream();
        internalClient.getObject(getObjectRequest).transferTo(readContents);
        assertThat(readContents.toString()).isEqualTo(fileContents);
    }

    @Test
    public void testPresignedDelete()
            throws URISyntaxException
    {
        uploadFileToStorage("three", "fileToDelete", TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket("three").key("fileToDelete").build();
            DeleteObjectPresignRequest presignRequest = DeleteObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .deleteObjectRequest(deleteObjectRequest)
                    .build();
            PresignedDeleteObjectRequest presignedRequest = presigner.presignDeleteObject(presignRequest);

            Request request = prepareDelete()
                    .setUri(presignedRequest.url().toURI())
                    .build();
            StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(204);
        }

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(() -> getFileFromStorage("three", "fileToDelete"))
                .extracting(S3Exception::statusCode)
                .isEqualTo(404);
    }

    @Test
    public void testExpiredSignature()
            throws URISyntaxException, InterruptedException, IOException
    {
        uploadFileToStorage("three", "fileToDeleteExpired", TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket("three").key("fileToDeleteExpired").build();
            DeleteObjectPresignRequest presignRequest = DeleteObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(1))
                    .deleteObjectRequest(deleteObjectRequest)
                    .build();
            PresignedDeleteObjectRequest presignedRequest = presigner.presignDeleteObject(presignRequest);

            Thread.sleep(Duration.ofSeconds(2));
            Request request = prepareDelete()
                    .setUri(presignedRequest.url().toURI())
                    .build();
            StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(401);
        }

        assertThat(getFileFromStorage("three", "fileToDeleteExpired")).isEqualTo(Files.readString(TEST_FILE));
    }

    @Test
    public void testMultipart()
            throws URISyntaxException, IOException
    {
        String bucketName = "one";
        String key = "multipart-upload";
        String dummyPayload = "foo bar baz";
        try (S3Presigner presigner = buildPresigner()) {
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            CreateMultipartUploadPresignRequest presignCreateMultipartUploadRequest = CreateMultipartUploadPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .createMultipartUploadRequest(createMultipartUploadRequest)
                    .build();
            PresignedCreateMultipartUploadRequest presignedCreateMultipartUploadRequest = presigner.presignCreateMultipartUpload(presignCreateMultipartUploadRequest);
            StringResponse startMultipartResponse = httpClient.execute(preparePost().setUri(presignedCreateMultipartUploadRequest.url().toURI()).build(), createStringResponseHandler());
            assertThat(startMultipartResponse.getStatusCode()).isEqualTo(200);

            String uploadId;
            try (JsonParser objectMapper = xmlMapper.createParser(startMultipartResponse.getBody())) {
                uploadId = ((TextNode) objectMapper.readValueAsTree().get("UploadId")).textValue();
            }

            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(1)
                    .build();
            UploadPartPresignRequest presignUploadPartRequest = UploadPartPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .uploadPartRequest(uploadPartRequest)
                    .build();
            PresignedUploadPartRequest presignedUploadPartRequest = presigner.presignUploadPart(presignUploadPartRequest);
            StatusResponse uploadPartResponse = httpClient.execute(preparePut().setUri(presignedUploadPartRequest.url().toURI()).setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(dummyPayload.getBytes(StandardCharsets.UTF_8))).build(), createStatusResponseHandler());
            assertThat(uploadPartResponse.getStatusCode()).isEqualTo(200);

            String eTag = uploadPartResponse.getHeader("etag");

            // If we provide a body for this request here, the AWS SDK will sign the contents even though it should not
            // That results in Minio rejecting the request
            CompleteMultipartUploadRequest.Builder completeMultipartUploadRequestBuilder = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId);

            // This is the signature for the request without a signed payload, just like all other presigned requests
            PresignedCompleteMultipartUploadRequest presignedCompleteMultipartUploadRequest = presignCompleteMultipartUpload(presigner, completeMultipartUploadRequestBuilder);

            byte[] completeMultipartUploadPayload = presignCompleteMultipartUpload(
                    presigner,
                    completeMultipartUploadRequestBuilder.multipartUpload(CompletedMultipartUpload.builder().parts(ImmutableList.of(CompletedPart.builder().partNumber(1).eTag(eTag).build())).build()))
                    .signedPayload().orElseThrow().asByteArray();

            Request.Builder completeMultipartUploadHttpRequestBuilder = preparePost().setUri(presignedCompleteMultipartUploadRequest.url().toURI());
            presignedCompleteMultipartUploadRequest.signedHeaders().forEach((header, values) -> values.forEach(value -> completeMultipartUploadHttpRequestBuilder.addHeader(header, value)));
            completeMultipartUploadHttpRequestBuilder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(completeMultipartUploadPayload));
            StringResponse completeMultipartResponse = httpClient.execute(
                            completeMultipartUploadHttpRequestBuilder.build(),
                            createStringResponseHandler());
            assertThat(completeMultipartResponse.getStatusCode()).isEqualTo(200);
        }
        assertThat(getFileFromStorage(bucketName, key)).isEqualTo(dummyPayload);
    }

    private PresignedCompleteMultipartUploadRequest presignCompleteMultipartUpload(S3Presigner presigner, CompleteMultipartUploadRequest.Builder completeMultipartUploadRequestBuilder)
    {
        return presigner.presignCompleteMultipartUpload(CompleteMultipartUploadPresignRequest.builder()
                .completeMultipartUploadRequest(completeMultipartUploadRequestBuilder.build())
                .signatureDuration(TEST_SIGNATURE_DURATION)
                .build());
    }

    private void uploadFileToStorage(String bucketName, String key, Path filePath)
    {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        PutObjectResponse putObjectResponse = storageClient.putObject(putObjectRequest, filePath);
        assertThat(putObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);
    }

    private String getFileFromStorage(String bucketName, String key)
            throws IOException
    {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        ByteArrayOutputStream readContents = new ByteArrayOutputStream();
        internalClient.getObject(getObjectRequest).transferTo(readContents);
        return readContents.toString();
    }

    private S3Presigner buildPresigner()
    {
        AwsBasicCredentials proxyCredentials = AwsBasicCredentials.create(testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey());
        return S3Presigner.builder().region(Region.US_EAST_1).endpointOverride(s3ProxyUrl).credentialsProvider(StaticCredentialsProvider.create(proxyCredentials)).build();
    }
}
