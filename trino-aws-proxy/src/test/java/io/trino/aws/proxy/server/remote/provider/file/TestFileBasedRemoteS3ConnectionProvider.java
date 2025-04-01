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
package io.trino.aws.proxy.server.remote.provider.file;

import com.google.inject.Inject;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.AbstractTestProxiedRequests;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.remote.provider.file.TestFileBasedRemoteS3ConnectionProvider.Filter;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.UUID;

import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_IDENTITY_CREDENTIAL;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static io.trino.aws.proxy.server.testing.containers.S3Container.POLICY_USER_CREDENTIAL;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, Filter.class})
public class TestFileBasedRemoteS3ConnectionProvider
        extends AbstractTestProxiedRequests
{
    private static final Credential AWS_PROXY_CLIENT_CREDENTIAL = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    public static final class Filter
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            File configFile;
            try {
                configFile = File.createTempFile("remote-s3-credentials", ".json");
                configFile.deleteOnExit();
                String jsonContent = """
                        {
                            "%s": {
                                "remoteCredential": {
                                    "accessKey": "%s",
                                    "secretKey": "%s"
                                },
                                "remoteSessionRole": {
                                    "region": "us-east-1",
                                    "roleArn": "minio-doesnt-care"
                                }
                            }
                        }
                        """.formatted(AWS_PROXY_CLIENT_CREDENTIAL.accessKey(), POLICY_USER_CREDENTIAL.accessKey(), POLICY_USER_CREDENTIAL.secretKey());
                Files.writeString(configFile.toPath(), jsonContent);
            }
            catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }

            return builder
                    .withoutTestingRemoteS3ConnectionProvider()
                    .withProperty("remote-s3-connection-provider.type", "file")
                    .withProperty("remote-s3-connection-provider.connections-file-path", configFile.getAbsolutePath());
        }
    }

    private final S3Client internalClient;

    @Inject
    public TestFileBasedRemoteS3ConnectionProvider(
            S3Client internalClient,
            @ForS3Container S3Client storageClient,
            TestingHttpServer httpServer,
            TrinoAwsProxyConfig config,
            TestingCredentialsRolesProvider testingCredentialsRolesProvider,
            TestingS3RequestRewriteController requestRewriteController)
    {
        super(buildClient(httpServer, config), storageClient, requestRewriteController);
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        testingCredentialsRolesProvider.addCredentials(new IdentityCredential(AWS_PROXY_CLIENT_CREDENTIAL, TESTING_IDENTITY_CREDENTIAL.identity()));
    }

    private static S3Client buildClient(TestingHttpServer httpServer, TrinoAwsProxyConfig config)
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(AWS_PROXY_CLIENT_CREDENTIAL.accessKey(), AWS_PROXY_CLIENT_CREDENTIAL.secretKey());
        return clientBuilder(UriBuilder.fromUri(httpServer.getBaseUrl()).path(config.getS3Path()).build())
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }

    @Test
    public void testProxy404ResponseWhenRemoteS3ConnectionNotFound()
    {
        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(internalClient::listBuckets)
                .extracting(S3Exception::statusCode)
                .isEqualTo(HttpStatus.NOT_FOUND.code());
    }
}
