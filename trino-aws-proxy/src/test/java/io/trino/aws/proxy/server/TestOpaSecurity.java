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
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.security.opa.OpaClient;
import io.trino.aws.proxy.spi.security.opa.OpaRequest;
import io.trino.aws.proxy.spi.security.opa.OpaS3SecurityFacade;
import jakarta.ws.rs.core.UriBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.aws.proxy.server.security.opa.OpaS3SecurityModule.OPA_S3_SECURITY_IDENTIFIER;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoAwsProxyTest(filters = TestOpaSecurity.Filter.class)
public class TestOpaSecurity
{
    private final HttpClient httpClient;
    private final S3Client s3Client;
    private final S3Client storageClient;
    private final String opaContainerHost;
    private final int opaContainerPort;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withProperty("s3-security.type", OPA_S3_SECURITY_IDENTIFIER)
                    .withProperty("opa-s3-security.server-base-uri", "http://localhost/v1/data")
                    .addModule(binder -> binder.bind(OpaS3SecurityFacade.class).to(TestingOpaS3SecurityFacade.class).in(Scopes.SINGLETON))
                    .withS3Container()
                    .withOpaContainer();
        }
    }

    public static class TestingOpaS3SecurityFacade
            implements OpaS3SecurityFacade
    {
        private final OpaClient opaClient;
        private final String opaContainerHost;
        private final int opaContainerPort;

        @Inject
        public TestingOpaS3SecurityFacade(OpaContainer container, OpaClient opaClient)
        {
            opaContainerHost = container.getHost();
            opaContainerPort = container.getPort();
            this.opaClient = requireNonNull(opaClient, "opaClient is null");
        }

        @Override
        public SecurityResponse apply(ParsedS3Request request, Optional<String> lowercaseAction, URI opaServerBaseUri, Optional<Identity> identity)
        {
            assertThat(identity).isPresent();
            if (request.keyInBucket().equals("default-allow")) {
                return SecurityResponse.SUCCESS;
            }
            if (request.keyInBucket().equals("default-deny")) {
                return SecurityResponse.FAILURE;
            }
            URI uri = UriBuilder.fromUri(opaServerBaseUri).host(opaContainerHost).port(opaContainerPort).path("test").path("allow").build();
            return opaClient.getSecurityResponse(new OpaRequest(uri, ImmutableMap.of("table", request.keyInBucket())));
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
    public TestOpaSecurity(@ForOpa HttpClient httpClient, S3Client s3Client, @ForS3Container S3Client storageClient, OpaContainer container)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        opaContainerHost = container.getHost();
        opaContainerPort = container.getPort();
    }

    @BeforeAll
    public void setup()
    {
        Request request = preparePut()
                .setUri(UriBuilder.newInstance().scheme("http").host(opaContainerHost).port(opaContainerPort).path("v1/policies/test").build())
                .setBodyGenerator(createStaticBodyGenerator(POLICY, StandardCharsets.UTF_8))
                .build();

        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(200);

        storageClient.createBucket(r -> r.bucket("opa-test-bucket"));
    }

    @Test
    public void testOPAPolicyAllowance() throws IOException {
        String bucket = "opa-test-bucket", key = "good", content = "test-content";
        storageClient.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromString(content));

        String expected = getFileFromStorage(s3Client, bucket, key);

        assertThat(expected).isEqualTo(content);
    }

    @Test
    public void testOPAPolicyDisAllowance()
    {
        String bucket = "opa-test-bucket", key = "bad", content = "test-content";
        storageClient.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromString(content));

        assertThatThrownBy(() -> s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(401);
    }

    @Test
    public void testFastrackSuccess() throws IOException {
        String bucket = "opa-test-bucket", key = "default-allow", content = "test-content";
        storageClient.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromString(content));

        String expected = getFileFromStorage(s3Client, bucket, key);

        assertThat(expected).isEqualTo(content);
    }

    @Test
    public void testFastrackFailure()
    {
        String bucket = "opa-test-bucket", key = "default-deny", content = "test-content";
        storageClient.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromString(content));

        assertThatThrownBy(() -> s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(401);
    }
}
