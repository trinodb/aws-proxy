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
package io.trino.aws.proxy.server.testing;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.ws.rs.core.UriBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Optional;

import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static java.util.Objects.requireNonNull;

public class TestingS3ClientProvider
        implements Provider<S3Client>
{
    private final URI proxyUri;
    private final Credential testingCredentials;
    private final boolean forcePathStyle;

    public record TestingS3ClientConfig(Optional<String> hostName, String s3Path)
    {
        public TestingS3ClientConfig
        {
            requireNonNull(hostName, "hostName is null");
            requireNonNull(s3Path, "s3Path is null");
        }

        @Inject
        public TestingS3ClientConfig(TrinoS3ProxyConfig config)
        {
            this(config.getS3HostName(), config.getS3Path());
        }
    }

    @Inject
    public TestingS3ClientProvider(TestingHttpServer httpServer, @ForTesting Credentials testingCredentials, TestingS3ClientConfig clientConfig)
    {
        URI localProxyServerUri = httpServer.getBaseUrl();
        this.proxyUri = clientConfig.hostName()
                .map(serverHostName -> UriBuilder.newInstance().host(serverHostName).port(localProxyServerUri.getPort()).scheme("http").path(clientConfig.s3Path()).build())
                .orElse(UriBuilder.fromUri(localProxyServerUri).path(clientConfig.s3Path()).build());
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null").emulated();
        this.forcePathStyle = clientConfig.hostName().isEmpty();
    }

    @Override
    public S3Client get()
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(testingCredentials.accessKey(), testingCredentials.secretKey());

        return clientBuilder(proxyUri)
                .credentialsProvider(() -> awsBasicCredentials)
                .forcePathStyle(forcePathStyle)
                .build();
    }
}
