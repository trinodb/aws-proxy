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
package io.trino.aws.proxy.server.credentials.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.aws.proxy.server.credentials.http.HttpCredentialsModule.HTTP_CREDENTIALS_PROVIDER_IDENTIFIER;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.bindIdentityType;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestHttpCredentialsProvider.Filter.class)
public class TestHttpCredentialsProvider
{
    private static final String DUMMY_EMULATED_ACCESS_KEY = "test-emulated-access-key";
    private static final String DUMMY_EMULATED_SECRET_KEY = "test-emulated-secret-key";
    private static final String DUMMY_REMOTE_ACCESS_KEY = "test-remote-access-key";
    private static final String DUMMY_REMOTE_SECRET_KEY = "test-remote-secret-key";

    private final CredentialsProvider credentialsProvider;
    private final HttpCredentialsServlet httpCredentialsServlet;
    private final HttpCredentialsProvider httpCredentialsProvider;

    public static class Filter
            implements BuilderFilter
    {
        private static String httpEndpointUri;

        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            TestingHttpServer httpCredentialsServer;
            Map<String, String> expectedHeaders = ImmutableMap.<String, String>builder()
                    .put("Authorization", "some-auth")
                    .put("Content-Type", "application/json")
                    .put("Some-Dummy-Header", "test,value")
                    .buildOrThrow();
            String headerConfigAsString = "Authorization: some-auth, Content-Type: application/json, Some-Dummy-Header:test,,value";

            HttpCredentialsServlet httpCredentialsServlet = new HttpCredentialsServlet(expectedHeaders);
            try {
                httpCredentialsServer = createTestingHttpCredentialsServer(httpCredentialsServlet);
                httpCredentialsServer.start();
                httpEndpointUri = httpCredentialsServer.getBaseUrl().toString();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to start test http credentials provider server", e);
            }
            return builder.withoutTestingCredentialsRoleProviders()
                    .addModule(new HttpCredentialsModule())
                    .addModule(binder -> bindIdentityType(binder, TestingIdentity.class))
                    .withProperty("credentials-provider.type", HTTP_CREDENTIALS_PROVIDER_IDENTIFIER)
                    .withProperty("credentials-provider.http.endpoint", httpEndpointUri)
                    .withProperty("credentials-provider.http.headers", headerConfigAsString)
                    .withProperty("credentials-provider.http.cache-size", "2")
                    .withProperty("credentials-provider.http.cache-ttl", "10m")
                    .addModule(binder -> binder.bind(HttpCredentialsServlet.class).toInstance(httpCredentialsServlet));
        }
    }

    @Inject
    public TestHttpCredentialsProvider(CredentialsProvider credentialsProvider, HttpCredentialsServlet httpCredentialsServlet, CredentialsProvider httpCredentialsProvider)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
        this.httpCredentialsServlet = requireNonNull(httpCredentialsServlet, "httpCredentialsServlet is null");
        this.httpCredentialsProvider = (HttpCredentialsProvider) requireNonNull(httpCredentialsProvider, "httpCredentialsProvider is null");
    }

    @BeforeEach
    public void resetRequestCounter()
    {
        httpCredentialsProvider.resetCache();
        httpCredentialsServlet.resetRequestCount();
    }

    @Test
    public void testValidCredentialsWithEmptySession()
    {
        testValidCredentials(Optional.empty());
    }

    @Test
    public void testValidCredentialsWithValidSession()
    {
        testValidCredentials(Optional.of("%s-token".formatted(DUMMY_EMULATED_ACCESS_KEY)));
    }

    @Test
    public void testCredentialsCached()
    {
        String validToken = "%s-token".formatted(DUMMY_EMULATED_ACCESS_KEY);
        // First credential is retrieved
        testValidCredentials(Optional.of(validToken), 1);

        // Second credential is retrieved - the access key is identical, but the session token is not
        testValidCredentials(Optional.empty(), 2);

        // Requesting these same two credentials again should not result in new HTTP calls
        testValidCredentials(Optional.of(validToken), 2);
        testValidCredentials(Optional.empty(), 2);

        // But if we request something else, it should still go to the HTTP endpoint
        assertThat(credentialsProvider.credentials("something-else", Optional.empty())).isEmpty();
        assertThat(httpCredentialsServlet.getRequestCount()).isEqualTo(3);
    }

    private void testValidCredentials(Optional<String> emulatedAccessToken)
    {
        testValidCredentials(emulatedAccessToken, 1);
    }

    private void testValidCredentials(Optional<String> emulatedAccessToken, int expectedRequestCount)
    {
        Credential expectedEmulated = new Credential(DUMMY_EMULATED_ACCESS_KEY, DUMMY_EMULATED_SECRET_KEY, emulatedAccessToken);
        Credential expectedRemote = new Credential(DUMMY_REMOTE_ACCESS_KEY, DUMMY_REMOTE_SECRET_KEY);

        Credentials expected = new Credentials(expectedEmulated, Optional.of(expectedRemote), Optional.empty(), Optional.of(new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq")));
        Optional<Credentials> actual = credentialsProvider.credentials(DUMMY_EMULATED_ACCESS_KEY, emulatedAccessToken);
        assertThat(actual).contains(expected);
        assertThat(httpCredentialsServlet.getRequestCount()).isEqualTo(expectedRequestCount);
    }

    @Test
    public void testInvalidCredentialsWithEmptySession()
    {
        testNoCredentialsRetrieved("non-existent-key", Optional.empty());
    }

    @Test
    public void testValidCredentialsWithInvalidSession()
    {
        testNoCredentialsRetrieved(DUMMY_EMULATED_ACCESS_KEY, Optional.of("session-token-not-valid-for-access-key"));
    }

    @Test
    public void testInvalidCredentialsWithInvalidSession()
    {
        testNoCredentialsRetrieved("non-existent-key", Optional.of("session-token-not-valid-for-access-key"));
    }

    @Test
    public void testIncorrectResponseFromServer()
    {
        testNoCredentialsRetrieved("incorrect-response", Optional.empty());
    }

    private void testNoCredentialsRetrieved(String emulatedAccessKey, Optional<String> sessionToken)
    {
        assertThat(credentialsProvider.credentials(emulatedAccessKey, sessionToken)).isEmpty();
        assertThat(httpCredentialsServlet.getRequestCount()).isEqualTo(1);
    }

    private static TestingHttpServer createTestingHttpCredentialsServer(HttpCredentialsServlet servlet)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer(httpServerInfo, nodeInfo, config, servlet, ImmutableMap.of());
    }

    private static class HttpCredentialsServlet
            extends HttpServlet
    {
        private final Map<String, String> expectedHeaders;
        private final AtomicInteger requestCounter;

        private HttpCredentialsServlet(Map<String, String> expectedHeaders)
        {
            this.expectedHeaders = ImmutableMap.copyOf(expectedHeaders);
            this.requestCounter = new AtomicInteger();
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            requestCounter.addAndGet(1);
            for (Map.Entry<String, String> expectedHeader : expectedHeaders.entrySet()) {
                if (!expectedHeader.getValue().equals(request.getHeader(expectedHeader.getKey()))) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    return;
                }
            }
            Optional<String> sessionToken = Optional.ofNullable(request.getParameter("sessionToken"));
            String emulatedAccessKey = request.getPathInfo().substring(1);
            // The session token in the request is legal if it is either:
            // - Not present
            // - Matching our test logic: it should be equal to the access-key + "-token"
            boolean isLegalSessionToken = sessionToken
                    .map(presentSessionToken -> "%s-token".formatted(emulatedAccessKey).equals(presentSessionToken))
                    .orElse(true);
            if (!isLegalSessionToken) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            switch (emulatedAccessKey) {
                case DUMMY_EMULATED_ACCESS_KEY -> {
                    Credential emulated = new Credential(DUMMY_EMULATED_ACCESS_KEY, DUMMY_EMULATED_SECRET_KEY, sessionToken);
                    Credential remote = new Credential(DUMMY_REMOTE_ACCESS_KEY, DUMMY_REMOTE_SECRET_KEY);
                    Credentials credentials = new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.of(new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq")));
                    String jsonCredentials = new ObjectMapperProvider().get().writeValueAsString(credentials);
                    response.setContentType(APPLICATION_JSON);
                    response.getWriter().print(jsonCredentials);
                }
                case "incorrect-response" -> {
                    response.getWriter().print("incorrect response");
                }
                default -> response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }

        private int getRequestCount()
        {
            return requestCounter.get();
        }

        private void resetRequestCount()
        {
            requestCounter.set(0);
        }
    }
}
