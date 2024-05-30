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

import com.google.common.io.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestProxiedRequests
{
    private final S3Client internalClient;
    private final S3Client remoteClient;
    private final List<String> configuredBuckets;

    protected AbstractTestProxiedRequests(S3Client internalClient, S3Client remoteClient, List<String> configuredBuckets)
    {
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.remoteClient = requireNonNull(remoteClient, "remoteClient is null");
        this.configuredBuckets = requireNonNull(configuredBuckets, "configuredBuckets is null");
    }

    @AfterEach
    public void cleanupBuckets()
    {
        remoteClient.listBuckets().buckets().forEach(bucket -> remoteClient.listObjectsV2Paginator(request -> request.bucket(bucket.name())).forEach(s3ObjectPage -> {
            if (s3ObjectPage.contents().isEmpty()) {
                return;
            }
            List<ObjectIdentifier> objectIdentifiers = s3ObjectPage.contents()
                    .stream()
                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                    .collect(toImmutableList());
            remoteClient.deleteObjects(deleteRequest -> deleteRequest.bucket(bucket.name()).delete(Delete.builder().objects(objectIdentifiers).build()));
        }));
    }

    @Test
    public void testListBuckets()
    {
        ListBucketsResponse listBucketsResponse = internalClient.listBuckets();
        assertThat(listBucketsResponse.buckets())
                .extracting(Bucket::name)
                .containsExactlyInAnyOrderElementsOf(configuredBuckets);

        assertThat(configuredBuckets.stream().map(bucketName -> internalClient.listObjects(request -> request.bucket(bucketName)).contents().size()))
                .containsOnly(0)
                .hasSize(configuredBuckets.size());
    }

    @Test
    public void testListBucketsWithContents()
    {
        String bucketToTest = configuredBuckets.getFirst();
        String testKey = "some-key";
        assertThat(internalClient.listObjects(request -> request.bucket(bucketToTest)).contents()).isEmpty();

        remoteClient.putObject(request -> request.bucket(bucketToTest).key(testKey), RequestBody.fromString("some-contents"));

        assertThat(internalClient.listObjects(request -> request.bucket(bucketToTest)).contents())
                .extracting(S3Object::key)
                .containsOnly(testKey);
    }

    @Test
    public void testUploadAndDelete()
            throws IOException
    {
        Path path = new File(Resources.getResource("testFile.txt").getPath()).toPath();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket("two").key("test").build();
        PutObjectResponse putObjectResponse = internalClient.putObject(putObjectRequest, path);
        assertThat(putObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("two").key("test").build();
        ByteArrayOutputStream readContents = new ByteArrayOutputStream();
        internalClient.getObject(getObjectRequest).transferTo(readContents);

        String expectedContents = Files.readString(path);

        assertThat(readContents.toString()).isEqualTo(expectedContents);

        ListObjectsResponse listObjectsResponse = internalClient.listObjects(request -> request.bucket("two"));
        assertThat(listObjectsResponse.contents())
                .hasSize(1)
                .first()
                .extracting(S3Object::key, S3Object::size)
                .containsExactlyInAnyOrder("test", Files.size(path));

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket("two").key("test").build();
        internalClient.deleteObject(deleteObjectRequest);

        listObjectsResponse = internalClient.listObjects(request -> request.bucket("two"));
        assertThat(listObjectsResponse.contents()).isEmpty();
    }
}
