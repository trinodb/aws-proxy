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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.units.Duration;
import io.trino.aws.proxy.server.credentials.CredentialsController;
import io.trino.aws.proxy.server.rest.RequestLoggerConfig;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.signing.InternalSigningController;
import io.trino.aws.proxy.server.signing.SigningControllerConfig;
import io.trino.aws.proxy.server.signing.TestingChunkSigningSession;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StreamingBodyGenerator.streamingBodyGenerator;
import static io.trino.aws.proxy.server.testing.TestingUtil.LOREM_IPSUM;
import static io.trino.aws.proxy.server.testing.TestingUtil.assertFileNotInS3;
import static io.trino.aws.proxy.server.testing.TestingUtil.deleteAllBuckets;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.headObjectInStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.sha256;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestGenericRestRequests.Filter.class)
public class TestHttpChunked
{
    private final URI baseUri;
    private final TestingCredentialsRolesProvider credentialsRolesProvider;
    private final HttpClient httpClient;
    private final Credentials testingCredentials;
    private final S3Client storageClient;

    private static final String TEST_CONTENT_TYPE = "text/plain;charset=utf-8";
    private static final Credential VALID_CREDENTIAL = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    @BeforeEach
    public void setupCredentials()
    {
        credentialsRolesProvider.addCredentials(Credentials.build(VALID_CREDENTIAL, testingCredentials.requiredRemoteCredential()));
    }

    @Inject
    public TestHttpChunked(
            TestingHttpServer httpServer,
            TestingCredentialsRolesProvider credentialsRolesProvider,
            @ForTesting HttpClient httpClient,
            @ForTesting Credentials testingCredentials,
            @ForS3Container S3Client storageClient,
            TrinoAwsProxyConfig trinoAwsProxyConfig)
    {
        baseUri = httpServer.getBaseUrl().resolve(trinoAwsProxyConfig.getS3Path());
        this.credentialsRolesProvider = requireNonNull(credentialsRolesProvider, "credentialsRolesProvider is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @AfterEach
    public void cleanupStorage()
    {
        deleteAllBuckets(storageClient);
    }

    private class ForceChunkInputStream
            extends InputStream
    {
        private final int chunkSize;
        private final Queue<InputStream> underlyingStreams;

        public ForceChunkInputStream(String allData, int chunkCount)
        {
            // We can have rounding errors due to the ceilDiv
            // Resulting in us having 1 less chunk
            checkArgument(chunkCount > 0, "chunkCount must be greater than 0");
            this.chunkSize = Math.ceilDiv(allData.length(), chunkCount);
            this.underlyingStreams = new LinkedList<>();
            chunkCount = Math.ceilDiv(allData.length(), this.chunkSize);
            for (int chunkPos = 0; chunkPos < chunkCount; chunkPos++) {
                underlyingStreams.add(new ByteArrayInputStream(allData.substring(chunkPos * chunkSize, Math.min((chunkPos + 1) * chunkSize, allData.length())).getBytes(StandardCharsets.UTF_8)));
            }
        }

        @Override
        public int read()
                throws IOException
        {
            if (underlyingStreams.isEmpty()) {
                return -1;
            }
            int result = underlyingStreams.peek().read();
            if (result == -1) {
                underlyingStreams.poll();
                return read();
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            if (underlyingStreams.isEmpty()) {
                return -1;
            }
            int result = underlyingStreams.peek().read(b, off, len);
            if (result == -1) {
                underlyingStreams.poll();
                return read(b, off, len);
            }
            return result;
        }
    }

    @Test
    public void testHttpChunked()
            throws IOException
    {
        String bucket = "test-http-chunked";
        String bucketTwo = "test-http-chunked-two";
        storageClient.createBucket(r -> r.bucket(bucket).build());
        testHttpChunked(bucket, LOREM_IPSUM, "UNSIGNED-PAYLOAD", 1);
        testHttpChunked(bucket, LOREM_IPSUM, "UNSIGNED-PAYLOAD", 3);
        testHttpChunked(bucket, LOREM_IPSUM, "UNSIGNED-PAYLOAD", 5);

        storageClient.createBucket(r -> r.bucket(bucketTwo).build());
        testHttpChunked(bucketTwo, LOREM_IPSUM, sha256(LOREM_IPSUM), 1);
        testHttpChunked(bucketTwo, LOREM_IPSUM, sha256(LOREM_IPSUM), 3);
        testHttpChunked(bucketTwo, LOREM_IPSUM, sha256(LOREM_IPSUM), 5);
    }

    private void testHttpChunked(String bucket, String content, String sha256, int partitionCount)
            throws IOException
    {
        assertThat(doHttpChunkedUpload(
                bucket,
                "basic-upload",
                content,
                partitionCount,
                ImmutableMultiMap.builder(false)
                        .add("X-Amz-Content-Sha256", sha256).build())).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "basic-upload")).isEqualTo(content);
        HeadObjectResponse basicUpload = headObjectInStorage(storageClient, bucket, "basic-upload");
        assertThat(basicUpload.contentEncoding()).isNullOrEmpty();
        assertThat(basicUpload.metadata()).isEmpty();

        assertThat(doHttpChunkedUpload(
                bucket,
                "with-content-type",
                content,
                partitionCount,
                ImmutableMultiMap.builder(false)
                        .add("X-Amz-Content-Sha256", sha256)
                        .add("Content-Type", TEST_CONTENT_TYPE)
                        .add("Content-Encoding", "gzip,compress")
                        .add("x-amz-meta-foobar", "baz")
                        .build())).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "with-content-type")).isEqualTo(content);
        HeadObjectResponse withFields = headObjectInStorage(storageClient, bucket, "with-content-type");
        assertThat(withFields.contentType()).contains(TEST_CONTENT_TYPE);
        assertThat(withFields.contentEncoding()).isEqualTo("gzip,compress");
        assertThat(withFields.metadata()).containsEntry("foobar", "baz");
    }

    @Test
    public void testHttpChunkedValidatesSignature()
    {
        String bucket = "http-chunked-wrong-signature";
        MultiMap requestHeaders = ImmutableMultiMap.builder(false)
                .add("X-Amz-Content-Sha256", sha256(LOREM_IPSUM + "foo"))
                .add("Content-Type", TEST_CONTENT_TYPE)
                .build();
        assertThat(doHttpChunkedUpload(bucket, "test-upload", LOREM_IPSUM, 3, requestHeaders)).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, "test-upload");
    }

    @Test
    public void testHttpChunkedContainingAwsChunkedPayload()
            throws IOException
    {
        String bucket = "http-chunked-aws-chunked";
        storageClient.createBucket(r -> r.bucket(bucket).build());

        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("Content-Encoding", "aws-chunked");
        assertThat(doCustomHttpChunkedUpload(bucket, "test-upload", 3, requestHeadersBuilder.build(), LOREM_IPSUM.length(), signature -> TestingChunkSigningSession.build(VALID_CREDENTIAL, signature).generateChunkedStream(LOREM_IPSUM, 3))).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "test-upload")).isEqualTo(LOREM_IPSUM);

        requestHeadersBuilder
                .add("Content-Type", TEST_CONTENT_TYPE)
                .add("Content-Encoding", "gzip,compress")
                .add("x-amz-meta-foobar", "baz");
        assertThat(doCustomHttpChunkedUpload(bucket, "test-upload-with-metadata", 3, requestHeadersBuilder.build(), LOREM_IPSUM.length(), signature -> TestingChunkSigningSession.build(VALID_CREDENTIAL, signature).generateChunkedStream(LOREM_IPSUM, 3))).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "test-upload-with-metadata")).isEqualTo(LOREM_IPSUM);
        HeadObjectResponse objectMetadata = headObjectInStorage(storageClient, bucket, "test-upload-with-metadata");
        assertThat(objectMetadata.contentType()).contains(TEST_CONTENT_TYPE);
        assertThat(objectMetadata.contentEncoding()).isEqualTo("gzip,compress");
        assertThat(objectMetadata.metadata()).containsEntry("foobar", "baz");
    }

    @Test
    public void testHttpChunkedContainingAwsChunkedPayloadValidatesChunkSignatures()
    {
        String bucket = "http-chunked-aws-chunked-errors";
        storageClient.createBucket(r -> r.bucket(bucket).build());

        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("Content-Encoding", "aws-chunked");
        assertThat(doCustomHttpChunkedUpload(
                bucket, "test-upload", 3, requestHeadersBuilder.build(), LOREM_IPSUM.length(),
                signature -> TestingChunkSigningSession.build(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), signature).generateChunkedStream(LOREM_IPSUM, 3))).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, "test-upload");
    }

    private int doHttpChunkedUpload(String bucket, String key, String content, int chunkCount, MultiMap extraHeaders)
    {
        return doCustomHttpChunkedUpload(bucket, key, chunkCount, extraHeaders, content.length(), _ -> content);
    }

    private int doCustomHttpChunkedUpload(String bucket, String key, int chunkCount, MultiMap extraHeaders, int decodedContentLength, Function<String, String> payloadBuilder)
    {
        Instant requestDate = Instant.now();
        ImmutableMultiMap.Builder requestHeaderBuilder = ImmutableMultiMap.builder(false)
                .add("Transfer-Encoding", "chunked")
                .add("X-Amz-Date", AwsTimestamp.toRequestFormat(requestDate))
                .add("X-Amz-Decoded-Content-Length", String.valueOf(decodedContentLength));
        extraHeaders.forEach(requestHeaderBuilder::addAll);

        URI requestUri = UriBuilder.fromUri(baseUri).path(bucket).path(key).build();

        InternalSigningController signingController = new InternalSigningController(
                new CredentialsController(new TestingRemoteS3Facade(), credentialsRolesProvider),
                new SigningControllerConfig().setMaxClockDrift(new Duration(10, TimeUnit.SECONDS)),
                new RequestLoggerController(new RequestLoggerConfig()));
        RequestAuthorization requestAuthorization = signingController.signRequest(new SigningMetadata(SigningServiceType.S3, Credentials.build(VALID_CREDENTIAL, testingCredentials.requiredRemoteCredential()), Optional.empty()),
                "us-east-1", requestDate, Optional.empty(), Credentials::emulated, requestUri, requestHeaderBuilder.build(), ImmutableMultiMap.empty(), "PUT").signingAuthorization();

        requestHeaderBuilder.add("Authorization", requestAuthorization.authorization());
        Request.Builder requestBuilder = preparePut()
                .setUri(requestUri)
                .setBodyGenerator(streamingBodyGenerator(new ForceChunkInputStream(payloadBuilder.apply(requestAuthorization.signature()), chunkCount)));
        requestHeaderBuilder.build().forEachEntry(requestBuilder::addHeader);

        return httpClient.execute(requestBuilder.build(), createStatusResponseHandler()).getStatusCode();
    }
}
