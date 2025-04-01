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

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import jakarta.ws.rs.core.UriBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class TestingS3ClientModule
        extends AbstractModule
{
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForVirtualHostProxy {}

    @Provides
    public S3Client getPathStyleAddressingClient(TestingHttpServer httpServer, @ForTesting IdentityCredential credentials, TrinoAwsProxyConfig config)
    {
        Credential emulatedCredentials = credentials.emulated();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(emulatedCredentials.accessKey(), emulatedCredentials.secretKey());
        return clientBuilder(UriBuilder.fromUri(httpServer.getBaseUrl()).path(config.getS3Path()).build())
                .credentialsProvider(() -> awsBasicCredentials)
                .forcePathStyle(true)
                .build();
    }

    @Provides
    @ForVirtualHostProxy
    public S3Client getVirtualHostAddressingClient(TestingHttpServer httpServer, @ForTesting IdentityCredential credentials, TrinoAwsProxyConfig config)
    {
        checkArgument(config.getS3HostName().isPresent(), "virtual host addressing proxy client requested but S3 hostname is not set");
        String hostname = config.getS3HostName().orElseThrow();
        Credential emulatedCredentials = credentials.emulated();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(emulatedCredentials.accessKey(), emulatedCredentials.secretKey());
        return clientBuilder(UriBuilder.newInstance().host(hostname).port(httpServer.getBaseUrl().getPort()).scheme("http").path(config.getS3Path()).build())
                .credentialsProvider(() -> awsBasicCredentials)
                .forcePathStyle(false)
                .build();
    }
}
