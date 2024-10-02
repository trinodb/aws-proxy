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
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingHttpCredentialsProviderServlet;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.aws.proxy.server.credentials.http.HttpCredentialsModule.HTTP_CREDENTIALS_PROVIDER_IDENTIFIER;
import static io.trino.aws.proxy.server.testing.TestingHttpCredentialsProviderServlet.DUMMY_EMULATED_ACCESS_KEY;
import static io.trino.aws.proxy.server.testing.TestingHttpCredentialsProviderServlet.DUMMY_EMULATED_SECRET_KEY;
import static io.trino.aws.proxy.server.testing.TestingHttpCredentialsProviderServlet.DUMMY_REMOTE_ACCESS_KEY;
import static io.trino.aws.proxy.server.testing.TestingHttpCredentialsProviderServlet.DUMMY_REMOTE_SECRET_KEY;
import static io.trino.aws.proxy.server.testing.TestingUtil.createTestingHttpServer;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.bindIdentityType;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestHttpCredentialsProvider.Filter.class)
public class TestHttpCredentialsProvider
{
    private final CredentialsProvider credentialsProvider;
    private final TestingHttpCredentialsProviderServlet httpCredentialsServlet;
    private final HttpCredentialsProvider httpCredentialsProvider;

    public static class Filter
            implements BuilderFilter
    {
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

            TestingHttpCredentialsProviderServlet httpCredentialsServlet;
            String httpEndpointUri;
            try {
                httpCredentialsServlet = new TestingHttpCredentialsProviderServlet(expectedHeaders);
                httpCredentialsServer = createTestingHttpServer(httpCredentialsServlet);
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
                    .addModule(binder -> binder.bind(TestingHttpCredentialsProviderServlet.class).toInstance(httpCredentialsServlet));
        }
    }

    @Inject
    public TestHttpCredentialsProvider(CredentialsProvider credentialsProvider, TestingHttpCredentialsProviderServlet httpCredentialsServlet, CredentialsProvider httpCredentialsProvider)
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
}
