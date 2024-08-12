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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.trino.aws.proxy.server.security.opa.ForOpa;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.OpaContainer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.opa.OpaRequest;
import io.trino.aws.proxy.spi.security.opa.OpaS3SecurityMapper;
import jakarta.ws.rs.core.UriBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.aws.proxy.server.security.opa.OpaS3SecurityModule.OPA_S3_SECURITY_IDENTIFIER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoAwsProxyTest(filters = TestOpaSecurity.Filter.class)
public class TestOpaSecurity
{
    private final HttpClient httpClient;
    private final int containerPort;
    private final S3Client s3Client;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withProperty("s3-security.type", OPA_S3_SECURITY_IDENTIFIER)
                    .withProperty("opa-s3-security.server-base-uri", "http://localhost/v1/data")
                    .addModule(binder -> binder.bind(OpaS3SecurityMapper.class).to(OpaMapper.class).in(Scopes.SINGLETON))
                    .withS3Container()
                    .withOpaContainer();
        }
    }

    public static class OpaMapper
            implements OpaS3SecurityMapper
    {
        private final int containerPort;

        @Inject
        public OpaMapper(OpaContainer container)
        {
            containerPort = container.getPort();
        }

        @Override
        public OpaRequest toRequest(ParsedS3Request request, Optional<String> lowercaseAction, URI baseUri)
        {
            URI uri = UriBuilder.fromUri(baseUri).port(containerPort).path("test").path("allow").build();
            return new OpaRequest(uri, ImmutableMap.of("table", request.keyInBucket()));
        }
    }

    private static final String POLICY = """
            package test

            default allow = false

            allow {
            	input.table = "good"
            }
            """;

    @Inject
    public TestOpaSecurity(@ForOpa HttpClient httpClient, OpaContainer container, S3Client s3Client)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        containerPort = container.getPort();
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
    }

    @BeforeAll
    public void setup()
    {
        Request request = preparePut()
                .setUri(URI.create("http://localhost:%s/v1/policies/test".formatted(containerPort)))
                .setBodyGenerator(createStaticBodyGenerator(POLICY, StandardCharsets.UTF_8))
                .build();

        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(200);
    }

    @Test
    public void testBadTable()
    {
        assertThatThrownBy(() -> s3Client.getObject(GetObjectRequest.builder().bucket("hey").key("bad").build()))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(401);
    }

    @Test
    public void testGoodTable()
    {
        assertThatThrownBy(() -> s3Client.getObject(GetObjectRequest.builder().bucket("hey").key("good").build()))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(404);    // table: good is allowed but does not exist
    }
}
