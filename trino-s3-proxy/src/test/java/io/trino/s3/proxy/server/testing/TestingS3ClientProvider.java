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
package io.trino.s3.proxy.server.testing;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.Credentials.Credential;
import io.trino.s3.proxy.server.rest.S3EndpointBuilder;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyResource;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import jakarta.ws.rs.core.UriBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.UUID;

import static io.trino.s3.proxy.server.rest.S3EndpointBuilder.buildEndpoint;
import static io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer.LOCAL_HOSTNAME;

public class TestingS3ClientProvider
        implements Provider<S3Client>
{
    private final Credentials credentials;
    private final URI localProxyServerUri;

    @SuppressWarnings("resource")
    @Inject
    public TestingS3ClientProvider(
            TestingTrinoS3ProxyServer trinoS3ProxyServer,
            ManagedS3MockContainer container,
            @ForS3MockContainer Credential containerCredential,
            TestingCredentialsController credentialsController,
            TestingS3EndpointBuilder endpointBuilder)
    {
        credentials = new Credentials(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), containerCredential);
        credentialsController.addCredentials(credentials);

        S3EndpointBuilder mockS3EndpointBuilder = (UriBuilder uriBuilder, String path, String bucket, String region) ->
                buildEndpoint(uriBuilder, LOCAL_HOSTNAME, container.container().getFirstMappedPort(), false, path, bucket, region);
        endpointBuilder.setDelegate(mockS3EndpointBuilder);

        URI baseUrl = trinoS3ProxyServer.getInjector().getInstance(TestingHttpServer.class).getBaseUrl();
        localProxyServerUri = UriBuilder.fromUri(baseUrl).host(LOCAL_HOSTNAME).path(TrinoS3ProxyResource.class).build();
    }

    @Override
    public S3Client get()
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(credentials.emulated().accessKey(), credentials.emulated().secretKey());

        return S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(() -> awsBasicCredentials)
                .endpointOverride(localProxyServerUri)
                .build();
    }
}
