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
package io.trino.aws.proxy.server.rest;

import com.google.common.io.ByteStreams;
import io.trino.aws.proxy.server.testing.TestingS3PresignController;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingS3SecurityController;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static io.trino.aws.proxy.spi.security.SecurityResponse.FAILURE;
import static io.trino.aws.proxy.spi.security.SecurityResponse.SUCCESS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestPresigningHeaders
{
    private final S3Client storageClient;
    private final S3Client internalClient;
    private final TestingS3SecurityController securityController;
    private final TestingS3RequestRewriteController s3RequestRewriteController;

    public AbstractTestPresigningHeaders(S3Client storageClient, S3Client internalClient, TestingS3PresignController presignController, TestingS3SecurityController securityController, TestingS3RequestRewriteController s3RequestRewriteController)
    {
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.securityController = requireNonNull(securityController, "securityController is null");
        this.s3RequestRewriteController = requireNonNull(s3RequestRewriteController, "s3RequestRewriteController is null");

        presignController.setRewriteUrisForContainers(false);
    }

    @AfterEach
    public void reset()
    {
        securityController.clear();
    }

    @Test
    public void testPresignHeaderGet()
            throws Exception
    {
        String bucketName = "one";
        String key = "getTest";
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3RequestRewriteController.getTargetBucket(bucketName, key))
                .key(s3RequestRewriteController.getTargetKey(bucketName, key))
                .build();
        storageClient.putObject(putObjectRequest, TEST_FILE);

        URI uri = getPresigned("get", bucketName, key).uri;

        // test the pre-signed URL by using it directly without any additional headers, signing, etc.
        try (InputStream inputStream = uri.toURL().openStream()) {
            String readContents = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
            String expectedContents = Files.readString(TEST_FILE);
            assertThat(readContents).isEqualTo(expectedContents);
        }
    }

    @Test
    public void testPresignHeaderSecurity()
    {
        String bucketName = "one";
        String key = "getTest";
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        internalClient.putObject(putObjectRequest, TEST_FILE);

        Presigned presigned = getPresigned("get", bucketName, key);
        assertThat(presigned.presignedHeaderMethods).containsExactlyInAnyOrder("GET", "PUT", "POST", "DELETE");

        securityController.setDelegate((request, _) -> lowercaseAction -> {
            // Authorization happens with the original bucket and key, not the rewritten one
            if (!request.bucketName().equals(bucketName) || !request.keyInBucket().equals(key)) {
                return FAILURE;
            }
            return request.httpVerb().equalsIgnoreCase("DELETE") ? FAILURE : SUCCESS;
        });

        presigned = getPresigned("get", bucketName, key);
        assertThat(presigned.presignedHeaderMethods).containsExactlyInAnyOrder("GET", "PUT", "POST");
    }

    @Test
    public void testPresignHeaderMultiPart()
            throws Exception
    {
        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder().bucket("three").key("multi").build();
        CreateMultipartUploadResponse multipartUploadResponse = internalClient.createMultipartUpload(multipartUploadRequest);

        String uploadId = multipartUploadResponse.uploadId();

        record Part(URI presignedUri, String content, int partNumber) {}

        List<Part> parts = IntStream.rangeClosed(1, 5).mapToObj(partNumber -> {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket("three")
                    .key("multi")
                    .partNumber(partNumber)
                    .overrideConfiguration(c -> c.putRawQueryParameter("uploadId", uploadId))
                    .build();
            URI uri = getPresigned("PUT", request).uri;
            String content = buildLine(partNumber);
            return new Part(uri, content, partNumber);
        }).collect(toImmutableList());

        List<CompletedPart> completedParts = parts.stream().map(part -> {
            try {
                String eTag = upload(part.presignedUri, part.content);
                return CompletedPart.builder().eTag(eTag).partNumber(part.partNumber).build();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(toImmutableList());

        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket("three")
                .key("multi")
                .uploadId(uploadId)
                .multipartUpload(completedUpload)
                .build();

        CompleteMultipartUploadResponse completeResponse = internalClient.completeMultipartUpload(completeRequest);
        assertThat(completeResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("three").key("multi").build();
        ByteArrayOutputStream readContents = new ByteArrayOutputStream();
        internalClient.getObject(getObjectRequest).transferTo(readContents);

        String expected = IntStream.rangeClosed(1, 5)
                .mapToObj(AbstractTestPresigningHeaders::buildLine)
                .collect(Collectors.joining());

        assertThat(readContents.toString()).isEqualTo(expected);
    }

    @Test
    public void testPresignHeaderPut()
            throws Exception
    {
        String bucketName = "two";
        String key = "puttest";
        URI uri = getPresigned("put", bucketName, key).uri;

        String fileContents = Files.readString(TEST_FILE);

        // test the pre-signed URL by using it directly without any additional headers, signing, etc.
        upload(uri, fileContents);

        // check that the file was uploaded correctly
        assertThat(getFileFromStorage(internalClient, bucketName, key)).isEqualTo(fileContents);
        assertThat(getFileFromStorage(storageClient, s3RequestRewriteController.getTargetBucket(bucketName, key), s3RequestRewriteController.getTargetKey(bucketName, key))).isEqualTo(fileContents);
    }

    private static String upload(URI uri, String contents)
            throws IOException
    {
        HttpURLConnection urlConnection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            urlConnection.setFixedLengthStreamingMode(contents.length());
            urlConnection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
            urlConnection.setDoOutput(true);
            urlConnection.setDoInput(true);
            urlConnection.setRequestMethod("PUT");
            urlConnection.connect();
            try (OutputStream outputStream = urlConnection.getOutputStream()) {
                ByteStreams.copy(new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8)), outputStream);
                outputStream.flush();
            }

            return urlConnection.getHeaderField("eTag");
        }
        finally {
            urlConnection.disconnect();
        }
    }

    private record Presigned(URI uri, Set<String> presignedHeaderMethods) {}

    private Presigned getPresigned(String type, String bucket, String key)
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return getPresigned(type, request);
    }

    private Presigned getPresigned(String type, HeadObjectRequest request)
    {
        SdkHttpResponse sdkHttpResponse;
        try {
            HeadObjectResponse response = internalClient.headObject(request);
            sdkHttpResponse = response.sdkHttpResponse();
        }
        catch (AwsServiceException e) {
            // when the bucket isn't found an exception is thrown - but response headers still have pre-signed URLs
            sdkHttpResponse = e.awsErrorDetails().sdkHttpResponse();
        }

        Set<String> presignedHeaderMethods = sdkHttpResponse.headers()
                .keySet()
                .stream()
                .filter(header -> header.toLowerCase(Locale.ROOT).startsWith("x-trino-pre-signed-url-"))
                .map(header -> header.substring("x-trino-pre-signed-url-".length()))
                .collect(toImmutableSet());

        // use an odd case for the header name on purpose
        String header = "x-TRINO-pre-SIGNED-uRl-" + type;
        String uri = sdkHttpResponse.firstMatchingHeader(header).orElseThrow();
        return new Presigned(URI.create(uri), presignedHeaderMethods);
    }

    private static String buildLine(int partNumber)
    {
        // min multi-part is 5MB
        return Character.toString('a' + (partNumber - 1)).repeat(1024 * 1024 * 5);
    }
}
