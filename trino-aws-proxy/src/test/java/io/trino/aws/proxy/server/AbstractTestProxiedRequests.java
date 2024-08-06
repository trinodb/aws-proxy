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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingUtil;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.headObjectInStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.listFilesInS3Bucket;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestProxiedRequests
{
    final S3Client internalClient;
    final S3Client remoteClient;
    private final TestingS3RequestRewriteController requestRewriteController;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    protected AbstractTestProxiedRequests(S3Client internalClient, S3Client remoteClient, TestingS3RequestRewriteController requestRewriteController)
    {
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.remoteClient = requireNonNull(remoteClient, "remoteClient is null");
        this.requestRewriteController = requireNonNull(requestRewriteController, "requestRewriteController is null");
    }

    @PreDestroy
    public void shutdown()
    {
        shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30));
    }

    @AfterEach
    public void cleanupBuckets()
    {
        TestingUtil.cleanupBuckets(remoteClient);
    }

    @Test
    public void testCreateBucket()
    {
        String newBucketName = "new-bucket";
        CreateBucketResponse createBucketResponse = internalClient.createBucket(r -> r.bucket(newBucketName));
        assertThat(createBucketResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        ListBucketsResponse listBucketsResponse = remoteClient.listBuckets();
        assertThat(listBucketsResponse.buckets()).extracting(Bucket::name).contains(requestRewriteController.getTargetBucket(newBucketName, ""));
    }

    @Test
    public void testListBuckets()
    {
        List<String> actualBuckets = remoteClient.listBuckets().buckets().stream().map(Bucket::name).collect(toImmutableList());
        List<String> bucketsReportedByProxy = internalClient.listBuckets().buckets().stream().map(Bucket::name).collect(toImmutableList());

        assertThat(bucketsReportedByProxy).containsExactlyElementsOf(actualBuckets);
    }

    @Test
    public void testListBucketsWithContents()
    {
        String bucketToTest = "one";
        String testKey = "some-key";
        assertThat(listFilesInS3Bucket(internalClient, bucketToTest)).isEmpty();

        remoteClient.putObject(request -> request.bucket(requestRewriteController.getTargetBucket(bucketToTest, testKey)).key(requestRewriteController.getTargetKey(bucketToTest, testKey)), RequestBody.fromString("some-contents"));

        assertThat(listFilesInS3Bucket(internalClient, bucketToTest)).containsExactlyInAnyOrder(requestRewriteController.getTargetKey(bucketToTest, testKey));
    }

    @Test
    public void testUploadAndDelete()
            throws IOException
    {
        String bucket = "two";
        String key = "test";
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(key).build();
        PutObjectResponse putObjectResponse = internalClient.putObject(putObjectRequest, TEST_FILE);
        assertThat(putObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        String expectedContents = Files.readString(TEST_FILE);
        assertThat(getFileFromStorage(internalClient, bucket, key)).isEqualTo(expectedContents);
        assertThat(getFileFromStorage(remoteClient, requestRewriteController.getTargetBucket(bucket, key), requestRewriteController.getTargetKey(bucket, key))).isEqualTo(expectedContents);

        assertThat(listFilesInS3Bucket(internalClient, bucket)).containsExactlyInAnyOrder(requestRewriteController.getTargetKey(bucket, key));
        assertThat(listFilesInS3Bucket(remoteClient, requestRewriteController.getTargetBucket(bucket, key))).containsExactlyInAnyOrder(requestRewriteController.getTargetKey(bucket, key));

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
        internalClient.deleteObject(deleteObjectRequest);

        assertThat(listFilesInS3Bucket(internalClient, bucket)).isEmpty();
        assertThat(listFilesInS3Bucket(remoteClient, requestRewriteController.getTargetBucket(bucket, key))).isEmpty();
    }

    @Test
    public void testUploadWithContentTypeAndMetadata()
            throws IOException
    {
        String bucket = "two";
        String key = "testWithMetadata";
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("text/plain;charset=utf-8")
                .contentEncoding("gzip,compress")
                .metadata(ImmutableMap.of("metadata-key", "metadata-value"))
                .build();
        PutObjectResponse putObjectResponse = internalClient.putObject(putObjectRequest, TEST_FILE);
        assertThat(putObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        assertThat(getFileFromStorage(internalClient, bucket, key)).isEqualTo(Files.readString(TEST_FILE));
        assertThat(getFileFromStorage(remoteClient, requestRewriteController.getTargetBucket(bucket, key), requestRewriteController.getTargetKey(bucket, key))).isEqualTo(Files.readString(TEST_FILE));
        HeadObjectResponse headObjectResponse = headObjectInStorage(internalClient, bucket, key);
        assertThat(headObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        assertThat(headObjectResponse.contentType()).isEqualTo("text/plain;charset=utf-8");
        assertThat(headObjectResponse.contentEncoding()).isEqualTo("gzip,compress");
        assertThat(headObjectResponse.metadata()).containsEntry("metadata-key", "metadata-value");
    }

    @Test
    public void testMultipartUpload()
            throws IOException
    {
        String bucket = "three";
        String key = "multi";
        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();
        CreateMultipartUploadResponse multipartUploadResponse = internalClient.createMultipartUpload(multipartUploadRequest);

        String uploadId = multipartUploadResponse.uploadId();

        List<CompletedPart> completedParts = new CopyOnWriteArrayList<>();

        List<? extends Future<?>> futures = IntStream.rangeClosed(1, 5)
                .mapToObj(partNumber -> executorService.submit(() -> {
                    String content = buildLine(partNumber);
                    UploadPartRequest part = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).contentLength((long) content.length()).build();
                    UploadPartResponse uploadPartResponse = internalClient.uploadPart(part, RequestBody.fromString(content));
                    assertThat(uploadPartResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

                    completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build());
                }))
                .collect(toImmutableList());

        futures.forEach(f -> {
            try {
                f.get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        List<CompletedPart> sortedCompletedParts = completedParts.stream()
                .sorted(Comparator.comparing(CompletedPart::partNumber))
                .collect(toImmutableList());

        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
                .parts(sortedCompletedParts)
                .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(completedUpload)
                .build();

        CompleteMultipartUploadResponse completeResponse = internalClient.completeMultipartUpload(completeRequest);
        assertThat(completeResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        String expected = IntStream.rangeClosed(1, 5)
                .mapToObj(AbstractTestProxiedRequests::buildLine)
                .collect(Collectors.joining());

        assertThat(getFileFromStorage(internalClient, bucket, key)).isEqualTo(expected);
        assertThat(getFileFromStorage(remoteClient, requestRewriteController.getTargetBucket(bucket, key), requestRewriteController.getTargetKey(bucket, key))).isEqualTo(expected);

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
        DeleteObjectResponse deleteObjectResponse = internalClient.deleteObject(deleteObjectRequest);
        assertThat(deleteObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(204);
    }

    @Test
    public void testPathsNeedingEscaping()
    {
        String bucket = "escapes";
        remoteClient.createBucket(r -> r.bucket(requestRewriteController.getTargetBucket(bucket, "")));
        internalClient.putObject(r -> r.bucket(bucket).key("a=1/b=2"), RequestBody.fromString("something"));
        internalClient.putObject(r -> r.bucket(bucket).key("a=1%2Fb=2"), RequestBody.fromString("else"));

        List<String> expectedKeys = ImmutableList.of(requestRewriteController.getTargetKey(bucket, "a=1/b=2"), requestRewriteController.getTargetKey(bucket, "a=1%2Fb=2"));
        assertThat(listFilesInS3Bucket(internalClient, bucket)).containsExactlyInAnyOrderElementsOf(expectedKeys);
        assertThat(listFilesInS3Bucket(remoteClient, requestRewriteController.getTargetBucket(bucket, ""))).containsExactlyInAnyOrderElementsOf(expectedKeys);

        internalClient.deleteObject(r -> r.bucket(bucket).key("a=1/b=2"));
        internalClient.deleteObject(r -> r.bucket(bucket).key("a=1%2Fb=2"));
        assertThat(listFilesInS3Bucket(internalClient, bucket)).isEmpty();
        internalClient.deleteBucket(r -> r.bucket(bucket));
    }

    private static String buildLine(int partNumber)
    {
        // min multi-part is 5MB
        return Character.toString('a' + (partNumber - 1)).repeat(1024 * 1024 * 5);
    }
}
