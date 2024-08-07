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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static io.trino.aws.proxy.server.credentials.http.HttpCredentialsModule.HTTP_CREDENTIALS_PROVIDER_IDENTIFIER;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.bindIdentityType;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestHttpCredentialsProvider.Filter.class)
public class TestHttpCredentialsProvider
{
    private final CredentialsProvider credentialsProvider;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            TestingHttpServer httpCredentialsServer;
            try {
                httpCredentialsServer = createTestingHttpCredentialsServer();
                httpCredentialsServer.start();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to start test http credentials provider server", e);
            }
            return builder.withoutTestingCredentialsRoleProviders()
                    .addModule(new HttpCredentialsModule())
                    .addModule(binder -> bindIdentityType(binder, TestingIdentity.class))
                    .withProperty("credentials-provider.type", HTTP_CREDENTIALS_PROVIDER_IDENTIFIER)
                    .withProperty("credentials-provider.http.endpoint", httpCredentialsServer.getBaseUrl().toString());
        }
    }

    @Inject
    public TestHttpCredentialsProvider(CredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
    }

    @Test
    public void testValidCredentialsWithEmptySession()
    {
        Credential emulated = new Credential("test-emulated-access-key", "test-emulated-secret");
        Credential remote = new Credential("test-remote-access-key", "test-remote-secret");
        Credentials expected = new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.of(new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq")));
        Optional<Credentials> actual = credentialsProvider.credentials("test-emulated-access-key", Optional.empty());
        assertThat(actual).contains(expected);
    }

    @Test
    public void testValidCredentialsWithValidSession()
    {
        Credential emulated = new Credential("test-emulated-access-key", "test-emulated-secret");
        Credential remote = new Credential("test-remote-access-key", "test-remote-secret");
        Credentials expected = new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.of(new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq")));
        Optional<Credentials> actual = credentialsProvider.credentials("test-emulated-access-key", Optional.of("test-emulated-access-key"));
        assertThat(actual).contains(expected);
    }

    @Test
    public void testInvalidCredentialsWithEmptySession()
    {
        Optional<Credentials> actual = credentialsProvider.credentials("non-existent-key", Optional.empty());
        assertThat(actual).isEmpty();
    }

    @Test
    public void testValidCredentialsWithInvalidSession()
    {
        Optional<Credentials> actual = credentialsProvider.credentials("test-emulated-access-key", Optional.of("sessionToken-not-equals-accessKey"));
        assertThat(actual).isEmpty();
    }

    @Test
    public void testInvalidCredentialsWithInvalidSession()
    {
        Optional<Credentials> actual = credentialsProvider.credentials("non-existent-key", Optional.of("sessionToken-not-equals-accessKey"));
        assertThat(actual).isEmpty();
    }

    @Test
    public void testIncorrectResponseFromServer()
    {
        Optional<Credentials> actual = credentialsProvider.credentials("incorrect-response", Optional.empty());
        assertThat(actual).isEmpty();
    }

    private static TestingHttpServer createTestingHttpCredentialsServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new HttpCredentialsServlet(), ImmutableMap.of());
    }

    private static class HttpCredentialsServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            Optional<String> sessionToken = Optional.ofNullable(request.getParameter("sessionToken"));
            String emulatedAccessKey = request.getPathInfo().substring(1);
            String credentialsIdentifier = "";
            if (sessionToken.isPresent()) {
                // Simulate valid session - When accessKey equals sessionToken
                if (emulatedAccessKey.equals(sessionToken.get())) {
                    credentialsIdentifier = sessionToken.get();
                }
            }
            else {
                credentialsIdentifier = emulatedAccessKey;
            }
            switch (credentialsIdentifier) {
                case "test-emulated-access-key" -> {
                    Credential emulated = new Credential("test-emulated-access-key", "test-emulated-secret");
                    Credential remote = new Credential("test-remote-access-key", "test-remote-secret");
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
    }
}
