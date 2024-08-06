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

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.units.Duration;
import io.trino.aws.proxy.server.credentials.CredentialsController;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.signing.InternalSigningController;
import io.trino.aws.proxy.server.signing.SigningControllerConfig;
import io.trino.aws.proxy.server.signing.TestingChunkSigningSession;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.aws.proxy.server.testing.TestingUtil.LOREM_IPSUM;
import static io.trino.aws.proxy.server.testing.TestingUtil.assertFileNotInS3;
import static io.trino.aws.proxy.server.testing.TestingUtil.deleteAllBuckets;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.headObjectInStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.listFilesInS3Bucket;
import static io.trino.aws.proxy.server.testing.TestingUtil.sha256;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestGenericRestRequests.Filter.class)
public class TestGenericRestRequests
{
    private final URI baseUri;
    private final TestingCredentialsRolesProvider credentialsRolesProvider;
    private final InternalSigningController signingController;
    private final HttpClient httpClient;
    private final Credentials testingCredentials;
    private final S3Client storageClient;

    private static final String TEST_CONTENT_TYPE = "text/plain;charset=utf-8";
    private static final String ILLEGAL_CHUNK_SIGNATURE = "0".repeat(AwsS3V4ChunkSigner.getSignatureLength());

    // first char is different case
    private static final String badContent = LOREM_IPSUM.toLowerCase(Locale.ROOT);

    private static final String goodSha256 = sha256(LOREM_IPSUM);

    private static final String badSha256 = sha256("foo" + LOREM_IPSUM);

    public static class Filter
            extends WithTestingHttpClient
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return super.filter(builder).withProperty("signing-controller.clock.max-drift", new Duration(9999999, TimeUnit.DAYS).toString());
        }
    }

    @Inject
    public TestGenericRestRequests(
            TestingHttpServer httpServer,
            TestingCredentialsRolesProvider credentialsRolesProvider,
            @ForTesting HttpClient httpClient,
            @ForTesting Credentials testingCredentials,
            @ForS3Container S3Client storageClient,
            TrinoAwsProxyConfig trinoAwsProxyConfig)
    {
        baseUri = httpServer.getBaseUrl().resolve(trinoAwsProxyConfig.getS3Path());
        this.credentialsRolesProvider = requireNonNull(credentialsRolesProvider, "credentialsRolesProvider is null");
        this.signingController = new InternalSigningController(
                new CredentialsController(new TestingRemoteS3Facade(), credentialsRolesProvider),
                new SigningControllerConfig().setMaxClockDrift(new Duration(10, TimeUnit.SECONDS)),
                new RequestLoggerController(new TrinoAwsProxyConfig()));
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @AfterEach
    public void cleanupStorage()
    {
        deleteAllBuckets(storageClient);
    }

    @Test
    public void testAwsChunkedUploadValid()
            throws IOException
    {
        String bucket = "test-aws-chunked";
        storageClient.createBucket(r -> r.bucket(bucket).build());

        Credential validCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        credentialsRolesProvider.addCredentials(Credentials.build(validCredential, testingCredentials.requiredRemoteCredential()));

        // Upload in 2 chunks
        assertThat(doAwsChunkedUpload(bucket, "aws-chunked-2-partitions", LOREM_IPSUM, 2, validCredential).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "aws-chunked-2-partitions")).isEqualTo(LOREM_IPSUM);

        // Upload in 3 chunks
        assertThat(doAwsChunkedUpload(bucket, "aws-chunked-3-partitions", LOREM_IPSUM, 3, validCredential).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "aws-chunked-3-partitions")).isEqualTo(LOREM_IPSUM);

        // Upload in 3 chunks with multiple content-encodings
        ImmutableMultiMap.Builder extraHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("x-amz-meta-metadata-key", "some-metadata-value")
                .add("Content-Encoding", "gzip,compress");
        assertThat(doAwsChunkedUpload(bucket, "aws-chunked-with-metadata", LOREM_IPSUM, 3, validCredential, validCredential, Function.identity(), extraHeadersBuilder.build()).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, "aws-chunked-with-metadata")).isEqualTo(LOREM_IPSUM);
        HeadObjectResponse headObjectResponse = headObjectInStorage(storageClient, bucket, "aws-chunked-with-metadata");
        assertThat(headObjectResponse.contentEncoding()).isEqualTo("gzip,compress");
        assertThat(headObjectResponse.contentType()).isEqualTo(TEST_CONTENT_TYPE);
        assertThat(headObjectResponse.metadata()).containsEntry("metadata-key", "some-metadata-value");
    }

    @Test
    public void testAwsChunkedUploadInvalidContent()
            throws IOException
    {
        String bucket = "test-aws-chunked";
        String fileKey = "sample_file_chunked";
        storageClient.createBucket(r -> r.bucket(bucket).build());

        Credential validCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        credentialsRolesProvider.addCredentials(Credentials.build(validCredential, testingCredentials.requiredRemoteCredential()));
        Credential validCredentialTwo = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        credentialsRolesProvider.addCredentials(Credentials.build(validCredentialTwo, testingCredentials.requiredRemoteCredential()));
        Credential unknownCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        // Credential is not known to the credential controller
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 2, unknownCredential).getStatusCode()).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, fileKey);

        // The request and the chunks are signed with different keys - both valid, but not matching
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 2, validCredential, validCredentialTwo, Function.identity(), ImmutableMultiMap.empty()).getStatusCode()).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, fileKey);

        // Final chunk has an invalid size
        Function<String, String> changeSizeOfFinalChunk = chunked -> chunked.replaceFirst("\\r\\n0;chunk-signature=(\\w+)", "\r\n1;chunk-signature=$1");
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 2, validCredential, changeSizeOfFinalChunk).getStatusCode()).isEqualTo(500);
        assertFileNotInS3(storageClient, bucket, fileKey);

        // First chunk has an invalid size
        Function<String, String> changeSizeOfFirstChunk = chunked -> {
            int firstChunkIdx = chunked.indexOf(";");
            String firstChunkSizeString = chunked.substring(0, firstChunkIdx);
            int firstChunkSize = Integer.parseInt(firstChunkSizeString.strip(), 16);
            int newSize = firstChunkSize - 1;
            String newSizeAsString = Integer.toString(newSize, 16);
            // We need to ensure the size (in string form) remains the same so the Content-Length is unchanged
            if (newSizeAsString.length() < firstChunkSizeString.length()) {
                newSizeAsString = "0" + newSizeAsString;
            }
            return "%s%s".formatted(newSizeAsString, chunked.substring(firstChunkIdx));
        };
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 2, validCredential, changeSizeOfFirstChunk).getStatusCode()).isEqualTo(500);
        assertFileNotInS3(storageClient, bucket, fileKey);

        // Change the signature of each of the chunks
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 3, validCredential, getMutatorToBreakSignatureForChunk(0)).getStatusCode()).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, fileKey);

        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 3, validCredential, getMutatorToBreakSignatureForChunk(1)).getStatusCode()).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, fileKey);

        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 3, validCredential, getMutatorToBreakSignatureForChunk(2)).getStatusCode()).isEqualTo(401);
        assertFileNotInS3(storageClient, bucket, fileKey);

        // Sanity check: uploads work with this key if we do not interfere
        assertThat(doAwsChunkedUpload(bucket, fileKey, LOREM_IPSUM, 2, validCredential).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, bucket, fileKey)).isEqualTo(LOREM_IPSUM);
    }

    private StatusResponse doAwsChunkedUpload(String bucket, String key, String contentToUpload, int partitionCount, Credential credential)
    {
        return doAwsChunkedUpload(bucket, key, contentToUpload, partitionCount, credential, Function.identity());
    }

    private StatusResponse doAwsChunkedUpload(String bucket, String key, String contentToUpload, int partitionCount, Credential credential, Function<String, String> chunkedPayloadMutator)
    {
        return doAwsChunkedUpload(bucket, key, contentToUpload, partitionCount, credential, credential, chunkedPayloadMutator, ImmutableMultiMap.empty());
    }

    private StatusResponse doAwsChunkedUpload(
            String bucket,
            String key,
            String contentToUpload,
            int partitionCount,
            Credential requestSigningCredential,
            Credential chunkSigningCredential,
            Function<String, String> chunkedPayloadMutator,
            MultiMap extraHeaders)
    {
        Instant requestDate = Instant.now();
        ImmutableMultiMap.Builder requestHeaderBuilder = ImmutableMultiMap.builder(false);
        extraHeaders.forEach(requestHeaderBuilder::addAll);
        requestHeaderBuilder
                .add("Host", "%s:%d".formatted(baseUri.getHost(), baseUri.getPort()))
                .add("X-Amz-Date", AwsTimestamp.toRequestFormat(requestDate))
                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("X-Amz-Decoded-Content-Length", String.valueOf(contentToUpload.length()))
                .add("Content-Length", String.valueOf(TestingChunkSigningSession.getExpectedChunkedStreamSize(contentToUpload, partitionCount)))
                .add("Content-Type", TEST_CONTENT_TYPE)
                .add("Content-Encoding", "aws-chunked");

        URI requestUri = UriBuilder.fromUri(baseUri).path(bucket).path(key).build();
        RequestAuthorization requestAuthorization = signRequest(requestSigningCredential, requestUri, requestDate, "PUT", requestHeaderBuilder.build());
        String chunkedContent = chunkedPayloadMutator.apply(TestingChunkSigningSession.build(chunkSigningCredential, requestAuthorization.signature(), requestDate).generateChunkedStream(contentToUpload, partitionCount));
        Request.Builder requestBuilder = preparePut().setUri(requestUri);

        requestHeaderBuilder.add("Authorization", requestAuthorization.authorization());
        requestHeaderBuilder.build().forEachEntry(requestBuilder::addHeader);
        requestBuilder.setBodyGenerator(createStaticBodyGenerator(chunkedContent.getBytes(StandardCharsets.UTF_8)));
        return httpClient.execute(requestBuilder.build(), createStatusResponseHandler());
    }

    @Test
    public void testAwsChunkedCornerCases()
            throws InterruptedException
    {
        String bucket = "test-aws-chunked-illegal";
        String dummyContent = "hello there";
        String longDummyContent = dummyContent.repeat(4096);
        storageClient.createBucket(r -> r.bucket(bucket).build());

        // Illegal signature and no final chunk
        testAwsChunkedIllegalChunks(bucket, "no-final-chunk", buildFakeChunk(longDummyContent, longDummyContent.length()), longDummyContent.length(), 500);
        // Illegal signature with a final chunk
        testAwsChunkedIllegalChunks(bucket, "with-final-chunk", "%s%s".formatted(buildFakeChunk(longDummyContent, longDummyContent.length()), buildFakeChunk("", 0)), longDummyContent.length(), 401);
        // Illegal signature and no final chunk - more chunked data than we report in the x-amz-decoded-content-length header
        testAwsChunkedIllegalChunks(bucket, "no-final-chunk-more-data-than-headers-indicate", buildFakeChunk(longDummyContent, longDummyContent.length()), 4096, 500);

        // Illegal signature with a final chunk - more chunked data than we report in the x-amz-decoded-content-length header
        testAwsChunkedIllegalChunks(bucket, "with-final-chunk-more-data-than-headers-indicate", "%s%s".formatted(buildFakeChunk(longDummyContent, longDummyContent.length()), buildFakeChunk("", 0)), 4096, 500);

        // Illegal signature and no final chunk - chunk misreports its size
        testAwsChunkedIllegalChunks(bucket, "no-final-chunk-chunk-underreports-size", buildFakeChunk(longDummyContent, 4096), 4096, 500);
        // Illegal signature with a final chunk - chunk misreports its size
        testAwsChunkedIllegalChunks(bucket, "with-final-chunk-chunk-underreports-size", "%s%s".formatted(buildFakeChunk(longDummyContent, 4096), buildFakeChunk("", 0)), 4096, 500);

        // Illegal signature and no final chunk - chunk misreports its size
        testAwsChunkedIllegalChunks(bucket, "no-final-chunk-chunk-overreports-size", buildFakeChunk(longDummyContent, 9_000_000), 4096, 500);
        // Illegal signature with a final chunk - chunk misreports its size
        testAwsChunkedIllegalChunks(bucket, "with-final-chunk-chunk-overreports-size", "%s%s".formatted(buildFakeChunk(longDummyContent, 9_000_000), buildFakeChunk("", 0)), 4096, 500);
        Thread.sleep(1000);
        assertThat(listFilesInS3Bucket(storageClient, bucket)).isEmpty();
    }

    private static String buildFakeChunk(String dataInChunk, int reportedChunkSize)
    {
        return "%s;chunk-signature=%s\r\n%s\r\n".formatted(Integer.toString(reportedChunkSize, 16), ILLEGAL_CHUNK_SIGNATURE, dataInChunk);
    }

    private void testAwsChunkedIllegalChunks(String bucket, String key, String rawContent, int decodedContentLength, int expectedStatusCode)
    {
        Instant requestDate = Instant.now();
        Credential validCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        credentialsRolesProvider.addCredentials(Credentials.build(validCredential, testingCredentials.requiredRemoteCredential()));

        ImmutableMultiMap.Builder requestHeaderBuilder = ImmutableMultiMap.builder(false);
        requestHeaderBuilder
                .add("Host", "%s:%d".formatted(baseUri.getHost(), baseUri.getPort()))
                .add("X-Amz-Date", AwsTimestamp.toRequestFormat(requestDate))
                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("X-Amz-Decoded-Content-Length", String.valueOf(decodedContentLength))
                .add("Content-Length", String.valueOf(rawContent.length()))
                .add("Content-Type", TEST_CONTENT_TYPE)
                .add("Content-Encoding", "aws-chunked");

        URI requestUri = UriBuilder.fromUri(baseUri).path(bucket).path(key).build();
        RequestAuthorization requestAuthorization = signRequest(validCredential, requestUri, requestDate, "PUT", requestHeaderBuilder.build());
        Request.Builder requestBuilder = preparePut().setUri(requestUri);

        requestHeaderBuilder.add("Authorization", requestAuthorization.authorization());
        requestHeaderBuilder.build().forEachEntry(requestBuilder::addHeader);
        requestBuilder.setBodyGenerator(createStaticBodyGenerator(rawContent.getBytes(StandardCharsets.UTF_8)));

        assertThat(httpClient.execute(requestBuilder.build(), createStatusResponseHandler()).getStatusCode()).isEqualTo(expectedStatusCode);
    }

    @Test
    public void testPutObject()
            throws IOException
    {
        String bucket = "foo";
        storageClient.createBucket(r -> r.bucket(bucket).build());

        testPutObject(bucket, LOREM_IPSUM, goodSha256, 200, true);
        testPutObject(bucket, LOREM_IPSUM, "UNSIGNED-PAYLOAD", 200, true);
        testPutObject(bucket, badContent, goodSha256, 401, false);
        testPutObject(bucket, LOREM_IPSUM, badSha256, 401, false);
        testPutObject(bucket, badContent, badSha256, 401, false);
    }

    private void testPutObject(String bucket, String content, String hash, int expectedStatusCode, boolean expectUpload)
            throws IOException
    {
        String fileKey = UUID.randomUUID().toString();
        assertThat(doPutObject(bucket, fileKey, content, hash).getStatusCode()).isEqualTo(expectedStatusCode);
        if (expectUpload) {
            assertThat(getFileFromStorage(storageClient, bucket, fileKey)).isEqualTo(content);
        }
        else {
            assertFileNotInS3(storageClient, bucket, fileKey);
        }
    }

    private StatusResponse doPutObject(String bucket, String key, String content, String sha256)
    {
        Instant requestDate = Instant.now();
        String requestDateStr = AwsTimestamp.toRequestFormat(requestDate);
        URI uri = UriBuilder.fromUri(baseUri).path(bucket).path(key).build();

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder headersBuilder = ImmutableMultiMap.builder(false)
                .add("Host", "%s:%d".formatted(uri.getHost(), uri.getPort()))
                .add("Accept-Encoding", "identity")
                .add("Content-Type", "text/plain")
                .add("User-Agent", "aws-cli/2.15.53 md/awscrt#0.19.19 ua/2.0 os/macos#22.6.0 md/arch#x86_64 lang/python#3.11.9 md/pyimpl#CPython cfg/retry-mode#standard md/installer#source md/prompt#off md/command#s3.cp")
                .add("Expect", "100-continue")
                .add("X-Amz-Date", requestDateStr)
                .add("X-Amz-Content-SHA256", sha256)
                .add("Content-Length", String.valueOf(content.length()));

        RequestAuthorization requestAuthorization = signRequest(testingCredentials.emulated(), uri, requestDate, "PUT", headersBuilder.build());
        headersBuilder.add("Authorization", requestAuthorization.authorization());
        Request.Builder requestBuilder = preparePut()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(content, StandardCharsets.UTF_8));
        headersBuilder.build().forEachEntry(requestBuilder::addHeader);

        return httpClient.execute(requestBuilder.build(), createStatusResponseHandler());
    }

    private RequestAuthorization signRequest(Credential signingCredential, URI uri, Instant requestDate, String method, MultiMap headers)
    {
        return signingController.signRequest(new SigningMetadata(SigningServiceType.S3, Credentials.build(signingCredential, testingCredentials.requiredRemoteCredential()), Optional.empty()),
                "us-east-1", requestDate, Optional.empty(), Credentials::emulated, uri, headers, ImmutableMultiMap.empty(), method).signingAuthorization();
    }

    private static Function<String, String> getMutatorToBreakSignatureForChunk(int chunkNumber)
    {
        return chunkedContent -> {
            int remainingChunks = chunkNumber;
            StringBuilder resultBuilder = new StringBuilder();
            List<String> parts = Splitter.on("\r\n").omitEmptyStrings().splitToList(chunkedContent);
            for (String part : parts) {
                if (part.contains(";chunk-signature=")) {
                    if (remainingChunks-- == 0) {
                        resultBuilder.append(part.replaceFirst("([0-9a-f]+;chunk-signature=)(\\w+)", "$1" + ILLEGAL_CHUNK_SIGNATURE));
                        resultBuilder.append("\r\n");
                        continue;
                    }
                }
                resultBuilder.append(part);
                resultBuilder.append("\r\n");
            }
            resultBuilder.append("\r\n");
            return resultBuilder.toString();
        };
    }
}
