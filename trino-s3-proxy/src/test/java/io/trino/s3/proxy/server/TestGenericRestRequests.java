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

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.units.Duration;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyRestConstants;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import io.trino.s3.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithTestingHttpClient;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoS3ProxyTest(filters = TestGenericRestRequests.Filter.class)
public class TestGenericRestRequests
{
    private final URI baseUri;
    private final TestingCredentialsRolesProvider credentialsRolesProvider;
    private final HttpClient httpClient;
    private final Credentials testingCredentials;
    private final S3Client storageClient;

    private static final String goodChunkedContent = """
                7b;chunk-signature=20e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\r
                0;chunk-signature=ae4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """;

    // first chunk-signature is bad
    private static final String badChunkedContent1 = """
                7b;chunk-signature=10e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\r
                0;chunk-signature=ae4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """;

    // second chunk-signature is bad
    private static final String badChunkedContent2 = """
                7b;chunk-signature=20e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\r
                0;chunk-signature=9e4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """;

    public static class Filter
            extends WithTestingHttpClient
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
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
            @ForS3MockContainer S3Client storageClient)
    {
        baseUri = httpServer.getBaseUrl();
        this.credentialsRolesProvider = requireNonNull(credentialsRolesProvider, "credentialsRolesProvider is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @Test
    public void testAwsChunkedUpload()
    {
        Credential credential = new Credential("c160cd8c-8273-4e34-bcf5-3dbddec0c6e0", "464cbc68-2d4f-4e4d-b653-5b1630db9f56");
        Credentials credentials = new Credentials(credential, testingCredentials.remote(), Optional.empty());
        credentialsRolesProvider.addCredentials(credentials);

        storageClient.createBucket(r -> r.bucket("two").build());

        assertThat(doAwsChunkedUpload(goodChunkedContent).getStatusCode()).isEqualTo(200);
        assertThat(doAwsChunkedUpload(badChunkedContent1).getStatusCode()).isEqualTo(401);
        assertThat(doAwsChunkedUpload(badChunkedContent2).getStatusCode()).isEqualTo(401);
    }

    private StatusResponse doAwsChunkedUpload(String content)
    {
        URI uri = UriBuilder.fromUri(baseUri)
                .replacePath(TrinoS3ProxyRestConstants.S3_PATH)
                .path("two")
                .path("test")
                .build();

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        Request request = preparePut().setUri(uri)
                .setHeader("Host", "127.0.0.1:62820")
                .setHeader("User-Agent", "aws-sdk-java/2.25.32 Mac_OS_X/13.6.7 OpenJDK_64-Bit_Server_VM/22.0.1+8-16 Java/22.0.1 kotlin/1.9.23-release-779 vendor/Oracle_Corporation io/sync http/Apache cfg/retry-mode/legacy")
                .setHeader("X-Amz-Date", "20240618T080640Z")
                .setHeader("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .setHeader("x-amz-decoded-content-length", "123")
                .setHeader("Authorization", "AWS4-HMAC-SHA256 Credential=c160cd8c-8273-4e34-bcf5-3dbddec0c6e0/20240618/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-encoding;content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=3bdce17ef4446ba2900c8f90b2e8ee812ccfa4625abb67030fae01dd1a9d347b")
                .setHeader("Content-Encoding", "aws-chunked")
                .setHeader("amz-sdk-invocation-id", "0c59609c-1c7b-e503-0583-b0271b5e8b21")
                .setHeader("amz-sdk-request", "attempt=1; max=4")
                .setHeader("Content-Length", "296")
                .setHeader("Content-Type", "text/plain")
                .setBodyGenerator(createStaticBodyGenerator(content, StandardCharsets.UTF_8))
                .build();

        return httpClient.execute(request, createStatusResponseHandler());
    }
}
