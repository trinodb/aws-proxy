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
package io.trino.aws.proxy.server.plugin.exception;

import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpStatus;
import io.trino.aws.proxy.server.credentials.DelegatingCredentialsProvider;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.credentialsProviderModule;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

@TrinoAwsProxyTest(filters = TestCredentialsProviderExceptionPropagation.Filter.class)
public class TestCredentialsProviderExceptionPropagation
{
    private final DelegatingCredentialsProvider delegatingCredentialsProvider;
    private final S3Client internalClient;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            return builder.withoutTestingCredentialsRoleProviders()
                    .addModule(credentialsProviderModule("testing", DelegatingCredentialsProvider.class, binder -> binder.bind(DelegatingCredentialsProvider.class).in(Scopes.SINGLETON)))
                    .withProperty("credentials-provider.type", "testing");
        }
    }

    @Inject
    public TestCredentialsProviderExceptionPropagation(DelegatingCredentialsProvider delegatingCredentialsProvider, S3Client internalClient)
    {
        this.delegatingCredentialsProvider = requireNonNull(delegatingCredentialsProvider, "delegatingCredentialsProvider is null");
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
    }

    @Test
    public void testRuntimeException()
    {
        delegatingCredentialsProvider.setDelegate((_, _) -> { throw new RuntimeException("Testing exception"); });
        assertThatThrownBy(internalClient::listBuckets)
                .asInstanceOf(type(S3Exception.class))
                .satisfies(s3Exception -> {
                    assertThat(s3Exception.statusCode()).isEqualTo(500);
                    assertThat(s3Exception.awsErrorDetails().errorMessage()).isEqualTo("Testing exception");
                });
    }

    @Test
    public void testWebApplicationException()
    {
        delegatingCredentialsProvider.setDelegate((_, _) -> { throw new WebApplicationException("Testing exception", HttpStatus.IM_A_TEAPOT.code()); });
        assertThatThrownBy(internalClient::listBuckets)
                .asInstanceOf(type(S3Exception.class))
                .satisfies(s3Exception -> {
                    assertThat(s3Exception.statusCode()).isEqualTo(418);
                    assertThat(s3Exception.awsErrorDetails().errorMessage()).isEqualTo("Testing exception");
                });
    }
}
