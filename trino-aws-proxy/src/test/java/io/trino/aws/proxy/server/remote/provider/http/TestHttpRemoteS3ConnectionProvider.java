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
package io.trino.aws.proxy.server.remote.provider.http;

import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.AbstractTestProxiedRequests;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_IDENTITY_CREDENTIAL;
import static io.trino.aws.proxy.server.testing.TestingUtil.createTestingHttpServer;
import static io.trino.aws.proxy.server.testing.containers.S3Container.POLICY_USER_CREDENTIAL;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, TestHttpRemoteS3ConnectionProvider.Filter.class})
public class TestHttpRemoteS3ConnectionProvider
        extends AbstractTestProxiedRequests
{
    public static final class Filter
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            TestingHttpRemoteS3ConnectionProviderServlet remoteS3ConnectionProviderServlet;
            URI httpEndpointUri;
            try {
                remoteS3ConnectionProviderServlet = new TestingHttpRemoteS3ConnectionProviderServlet();
                TestingHttpServer server = createTestingHttpServer(remoteS3ConnectionProviderServlet);
                server.start();
                httpEndpointUri = server.getBaseUrl();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to start test http remote s3 connection provider server", e);
            }

            return builder.withoutTestingRemoteS3ConnectionProvider()
                    .withProperty("remote-s3-connection-provider.type", "http")
                    .withProperty("remote-s3-connection-provider.http.endpoint", UriBuilder.fromUri(httpEndpointUri).path("/api/v1/remote_s3_connection").build().toString())
                    .withProperty("remote-s3-connection-provider.http.cache-size", "100")
                    .withProperty("remote-s3-connection-provider.http.cache-ttl", "5m")
                    .addModule(binder -> binder.bind(TestingHttpRemoteS3ConnectionProviderServlet.class).toInstance(remoteS3ConnectionProviderServlet));
        }
    }

    private final TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet;
    private final HttpRemoteS3ConnectionProvider httpRemoteS3ConnectionProvider;

    @Inject
    public TestHttpRemoteS3ConnectionProvider(S3Client internalClient,
            @ForS3Container S3Client storageClient,
            TestingS3RequestRewriteController requestRewriteController,
            TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet,
            RemoteS3ConnectionProvider httpRemoteS3ConnectionProvider)
    {
        super(internalClient, storageClient, requestRewriteController);
        this.testingHttpRemoteS3ConnectionProviderServlet = requireNonNull(testingHttpRemoteS3ConnectionProviderServlet, "testingHttpRemoteS3ConnectionProviderServlet is null");
        this.httpRemoteS3ConnectionProvider = (HttpRemoteS3ConnectionProvider) requireNonNull(httpRemoteS3ConnectionProvider, "httpRemoteS3ConnectionProvider is null");
    }

    @BeforeEach
    public void cleanup()
    {
        httpRemoteS3ConnectionProvider.resetCache();
        testingHttpRemoteS3ConnectionProviderServlet.reset();
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {
                    "remoteCredential": {
                        "accessKey": "%s",
                        "secretKey": "%s"
                    },
                    "remoteSessionRole": {
                        "region": "us-east-1",
                        "roleArn": "minio-doesnt-care"
                    }
                }
                """.formatted(POLICY_USER_CREDENTIAL.accessKey(), POLICY_USER_CREDENTIAL.secretKey()));
    }

    @AfterEach
    public void checkRemoteS3ConnectionProviderHttpServletRequests()
    {
        assertThat(testingHttpRemoteS3ConnectionProviderServlet.getRequestParameters()).hasSizeGreaterThan(0).allSatisfy(queryParameters -> {
            assertThat(queryParameters.get("emulated_access_key")).hasSize(1).first().isEqualTo(TESTING_IDENTITY_CREDENTIAL.emulated().accessKey());
            assertThat(queryParameters.get("identity")).hasSize(1).first()
                    .extracting(parameter -> jsonCodec(TestingIdentity.class).fromJson(parameter))
                    .isEqualTo(TESTING_IDENTITY_CREDENTIAL.identity().orElseThrow());
            assertThat(queryParameters.get("key")).hasSize(1);
            assertThat(queryParameters.get("bucket")).hasSize(1);
        });
    }

    public static final class TestingHttpRemoteS3ConnectionProviderServlet
            extends HttpServlet
    {
        private final List<Multimap<String, String>> requestParameters = new ArrayList<>();

        private Optional<String> response = Optional.empty();
        private Optional<HttpStatus> responseStatus = Optional.empty();

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws IOException
        {
            requestParameters.add(req.getParameterMap().entrySet().stream().collect(
                    flatteningToImmutableListMultimap(Map.Entry::getKey, value -> stream(value.getValue()))));

            if (!req.getPathInfo().equals("/api/v1/remote_s3_connection")) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found");
                return;
            }

            if (responseStatus.isPresent()) {
                HttpStatus actualResponseStatus = responseStatus.orElseThrow();
                resp.sendError(actualResponseStatus.code(), actualResponseStatus.reason());
                return;
            }

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType("application/json");
            resp.getWriter().write(response.orElseThrow());
        }

        public void setResponse(String response)
        {
            this.response = Optional.of(response);
        }

        public void setResponseStatusOverride(HttpStatus status)
        {
            this.responseStatus = Optional.of(status);
        }

        public void reset()
        {
            requestParameters.clear();
            response = Optional.empty();
            responseStatus = Optional.empty();
        }

        public List<Multimap<String, String>> getRequestParameters()
        {
            return requestParameters;
        }
    }
}
