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
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingUtil;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.headObjectInStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.listFilesInS3Bucket;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestPresignedRequests
{
    private final HttpClient httpClient;
    private final S3Client internalClient;
    private final S3Client storageClient;
    private final Credentials testingCredentials;
    private final URI s3ProxyUrl;
    private final XmlMapper xmlMapper;
    private final TestingS3RequestRewriteController requestRewriteController;

    private static final Duration TEST_SIGNATURE_DURATION = Duration.ofMinutes(10);

    protected AbstractTestPresignedRequests(
            HttpClient httpClient,
            S3Client internalClient,
            S3Client storageClient,
            Credentials testingCredentials,
            TestingHttpServer httpServer,
            TrinoAwsProxyConfig s3ProxyConfig,
            XmlMapper xmlMapper,
            TestingS3RequestRewriteController requestRewriteController)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.s3ProxyUrl = httpServer.getBaseUrl().resolve(s3ProxyConfig.getS3Path());
        this.xmlMapper = requireNonNull(xmlMapper, "xmlMapper is null");
        this.requestRewriteController = requireNonNull(requestRewriteController, "requestRewriteController is null");
    }

    @Test
    public void testPresignedGet()
            throws IOException
    {
        String bucketName = "one";
        String key = "presignedGet";
        uploadFileToStorage(requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key), TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .getObjectRequest(objectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
            StringResponse response = executeHttpRequest(presignedRequest.httpRequest(), createStringResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
            assertThat(response.getBody()).isEqualTo(Files.readString(TEST_FILE));
        }
    }

    @Test
    public void testPresignedPut()
            throws IOException
    {
        String bucketName = "two";
        String key = "presignedPut";
        String fileContents = Files.readString(TEST_FILE, StandardCharsets.UTF_8);

        try (S3Presigner presigner = buildPresigner()) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentEncoding("gzip")
                    .contentType("text/plain;charset=UTF-8")
                    .build();
            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .putObjectRequest(putObjectRequest)
                    .build();
            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

            StatusResponse response = executeHttpRequest(presignedRequest.httpRequest(), fileContents, createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(200);
        }

        assertThat(getFileFromStorage(bucketName, key)).isEqualTo(fileContents);
        HeadObjectResponse headObjectResponse = headObjectInStorage(storageClient, requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key));
        assertThat(headObjectResponse.contentType()).isEqualTo("text/plain;charset=UTF-8");
        assertThat(headObjectResponse.contentEncoding()).isEqualTo("gzip");
    }

    @Test
    public void testPresignedDelete()
    {
        String bucketName = "three";
        String key = "fileToDelete";
        uploadFileToStorage(requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key), TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
            DeleteObjectPresignRequest presignRequest = DeleteObjectPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .deleteObjectRequest(deleteObjectRequest)
                    .build();
            PresignedDeleteObjectRequest presignedRequest = presigner.presignDeleteObject(presignRequest);

            StatusResponse response = executeHttpRequest(presignedRequest.httpRequest(), createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(204);
        }

        assertThat(listFilesInS3Bucket(storageClient, requestRewriteController.getTargetBucket(bucketName, key))).isEmpty();
    }

    @Test
    public void testExpiredSignature()
            throws InterruptedException, IOException
    {
        String bucketName = "three";
        String key = "fileToDeleteExpired";
        uploadFileToStorage(requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key), TEST_FILE);

        try (S3Presigner presigner = buildPresigner()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
            DeleteObjectPresignRequest presignRequest = DeleteObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(1))
                    .deleteObjectRequest(deleteObjectRequest)
                    .build();
            PresignedDeleteObjectRequest presignedRequest = presigner.presignDeleteObject(presignRequest);

            Thread.sleep(Duration.ofSeconds(2));

            StatusResponse response = executeHttpRequest(presignedRequest.httpRequest(), createStatusResponseHandler());
            assertThat(response.getStatusCode()).isEqualTo(401);
        }

        assertThat(getFileFromStorage(bucketName, key)).isEqualTo(Files.readString(TEST_FILE));
    }

    @Test
    public void testMultipart()
            throws IOException
    {
        String bucketName = "one";
        String key = "multipart-upload";
        String dummyPayload = "foo bar baz";
        try (S3Presigner presigner = buildPresigner()) {
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .contentType("text/plain;charset=UTF-8")
                    .contentEncoding("gzip")
                    .metadata(ImmutableMap.of("some-metadata-key", "some-metadata-value"))
                    .key(key)
                    .build();
            CreateMultipartUploadPresignRequest presignCreateMultipartUploadRequest = CreateMultipartUploadPresignRequest.builder()
                    .signatureDuration(TEST_SIGNATURE_DURATION)
                    .createMultipartUploadRequest(createMultipartUploadRequest)
                    .build();
            PresignedCreateMultipartUploadRequest presignedCreateMultipartUploadRequest = presigner.presignCreateMultipartUpload(presignCreateMultipartUploadRequest);
            StringResponse startMultipartResponse = executeHttpRequest(presignedCreateMultipartUploadRequest.httpRequest(), createStringResponseHandler());
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
            StatusResponse uploadPartResponse = executeHttpRequest(presignedUploadPartRequest.httpRequest(), dummyPayload, createStatusResponseHandler());
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

            String completeMultipartUploadPayload = presignCompleteMultipartUpload(
                    presigner,
                    completeMultipartUploadRequestBuilder.multipartUpload(CompletedMultipartUpload.builder().parts(ImmutableList.of(CompletedPart.builder().partNumber(1).eTag(eTag).build())).build()))
                    .signedPayload().orElseThrow().asUtf8String();

            StatusResponse completeMultipartResponse = executeHttpRequest(presignedCompleteMultipartUploadRequest.httpRequest(), completeMultipartUploadPayload, createStatusResponseHandler());
            assertThat(completeMultipartResponse.getStatusCode()).isEqualTo(200);
        }
        assertThat(getFileFromStorage(bucketName, key)).isEqualTo(dummyPayload);
        HeadObjectResponse headResult = TestingUtil.headObjectInStorage(storageClient, requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key));
        assertThat(headResult.contentEncoding()).isEqualTo("gzip");
        assertThat(headResult.contentType()).isEqualTo("text/plain;charset=UTF-8");
        assertThat(headResult.metadata()).containsEntry("some-metadata-key", "some-metadata-value");
    }

    private PresignedCompleteMultipartUploadRequest presignCompleteMultipartUpload(S3Presigner presigner, CompleteMultipartUploadRequest.Builder completeMultipartUploadRequestBuilder)
    {
        return presigner.presignCompleteMultipartUpload(CompleteMultipartUploadPresignRequest.builder()
                .completeMultipartUploadRequest(completeMultipartUploadRequestBuilder.build())
                .signatureDuration(TEST_SIGNATURE_DURATION)
                .build());
    }

    void uploadFileToStorage(String bucketName, String key, Path filePath)
    {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        PutObjectResponse putObjectResponse = storageClient.putObject(putObjectRequest, filePath);
        assertThat(putObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);
    }

    String getFileFromStorage(String bucketName, String key)
            throws IOException
    {
        String dataFromProxy = TestingUtil.getFileFromStorage(internalClient, bucketName, key);
        String dataFromStorage = TestingUtil.getFileFromStorage(storageClient, requestRewriteController.getTargetBucket(bucketName, key), requestRewriteController.getTargetKey(bucketName, key));
        assertThat(dataFromProxy).isEqualTo(dataFromStorage);
        return dataFromStorage;
    }

    <T> T executeHttpRequest(SdkHttpRequest sdkRequest, ResponseHandler<T, RuntimeException> responseHandler)
    {
        return executeHttpRequest(sdkRequest, Optional.empty(), responseHandler);
    }

    <T> T executeHttpRequest(SdkHttpRequest sdkRequest, String body, ResponseHandler<T, RuntimeException> responseHandler)
    {
        return executeHttpRequest(sdkRequest, Optional.of(body), responseHandler);
    }

    <T> T executeHttpRequest(SdkHttpRequest sdkRequest, Optional<String> body, ResponseHandler<T, RuntimeException> responseHandler)
    {
        Request.Builder requestBuilder = switch (sdkRequest.method()) {
            case POST -> preparePost();
            case PUT -> preparePut();
            case GET -> prepareGet();
            case HEAD -> prepareHead();
            case DELETE -> prepareDelete();
            default -> throw new IllegalStateException("Unexpected HTTP method");
        };
        requestBuilder.setUri(sdkRequest.getUri());
        body.ifPresent(actualBody -> requestBuilder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(actualBody, StandardCharsets.UTF_8)));
        sdkRequest.forEachHeader((headerName, headerValues) -> headerValues.forEach(headerValue -> requestBuilder.addHeader(headerName, headerValue)));
        return httpClient.execute(requestBuilder.build(), responseHandler);
    }

    S3Presigner buildPresigner()
    {
        return buildPresigner(testingCredentials.emulated());
    }

    S3Presigner buildPresigner(Credential credential)
    {
        AwsBasicCredentials proxyCredentials = AwsBasicCredentials.create(credential.accessKey(), credential.secretKey());
        return S3Presigner.builder().region(Region.US_EAST_1).endpointOverride(s3ProxyUrl).credentialsProvider(StaticCredentialsProvider.create(proxyCredentials)).build();
    }
}
