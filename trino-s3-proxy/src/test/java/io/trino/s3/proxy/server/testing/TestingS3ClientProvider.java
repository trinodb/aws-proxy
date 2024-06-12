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

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyResource;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.s3.proxy.spi.credentials.Credential;
import io.trino.s3.proxy.spi.credentials.Credentials;
import jakarta.ws.rs.core.UriBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Optional;

import static io.trino.s3.proxy.server.testing.TestingUtil.clientBuilder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class TestingS3ClientProvider
        implements Provider<S3Client>
{
    private final URI proxyUri;
    private final Credential testingCredentials;
    private final boolean forcePathStyle;

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForS3ClientProvider {}

    @Inject
    public TestingS3ClientProvider(TestingHttpServer httpServer, @ForTesting Credentials testingCredentials, @ForS3ClientProvider Optional<String> hostName)
    {
        URI localProxyServerUri = httpServer.getBaseUrl();
        this.proxyUri = requireNonNull(hostName, "hostName is null")
                .map(serverHostName -> UriBuilder.newInstance().host(serverHostName).port(localProxyServerUri.getPort()).scheme("http").path(TrinoS3ProxyResource.class).build())
                .orElse(UriBuilder.fromUri(localProxyServerUri).path(TrinoS3ProxyResource.class).build());
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null").emulated();
        this.forcePathStyle = hostName.isEmpty();
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
