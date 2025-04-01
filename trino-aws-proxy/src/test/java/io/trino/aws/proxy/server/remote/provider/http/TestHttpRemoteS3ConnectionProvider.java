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
package io.trino.aws.proxy.server.remote.provider.http;

import com.google.inject.Inject;
import io.airlift.http.client.HttpStatus;
import io.trino.aws.proxy.server.AbstractTestProxiedRequests;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_IDENTITY_CREDENTIAL;
import static io.trino.aws.proxy.server.testing.containers.S3Container.POLICY_USER_CREDENTIAL;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, WithHttpRemoteS3ConnectionProvider.class})
public class TestHttpRemoteS3ConnectionProvider
        extends AbstractTestProxiedRequests
{
    private final S3Client internalClient;
    private final TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet;
    private final HttpRemoteS3ConnectionProvider httpRemoteS3ConnectionProvider;

    @Inject
    public TestHttpRemoteS3ConnectionProvider(S3Client internalClient,
            @ForS3Container S3Client storageClient,
            TestingS3RequestRewriteController requestRewriteController,
            TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet,
            RemoteS3ConnectionProvider httpRemoteS3ConnectionProvider)
    {
        super(internalClient, storageClient, requestRewriteController);
        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.testingHttpRemoteS3ConnectionProviderServlet = requireNonNull(testingHttpRemoteS3ConnectionProviderServlet, "testingHttpRemoteS3ConnectionProviderServlet is null");
        this.httpRemoteS3ConnectionProvider = (HttpRemoteS3ConnectionProvider) requireNonNull(httpRemoteS3ConnectionProvider, "httpRemoteS3ConnectionProvider is null");
    }

    @BeforeEach
    public void cleanup()
    {
        httpRemoteS3ConnectionProvider.resetCache();
        testingHttpRemoteS3ConnectionProviderServlet.reset();
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {
                    "remoteCredential": {
                        "accessKey": "%s",
                        "secretKey": "%s"
                    },
                    "remoteSessionRole": {
                        "region": "us-east-1",
                        "roleArn": "minio-doesnt-care"
                    }
                }
                """.formatted(POLICY_USER_CREDENTIAL.accessKey(), POLICY_USER_CREDENTIAL.secretKey()));
    }

    @AfterEach
    public void checkRemoteS3ConnectionProviderHttpServletRequests()
    {
        assertThat(testingHttpRemoteS3ConnectionProviderServlet.getRequestParameters()).hasSizeGreaterThan(0).allSatisfy(queryParameters -> {
            assertThat(queryParameters.get("emulated_access_key")).hasSize(1).first().isEqualTo(TESTING_IDENTITY_CREDENTIAL.emulated().accessKey());
            assertThat(queryParameters.get("identity")).hasSize(1).first()
                    .extracting(parameter -> jsonCodec(TestingIdentity.class).fromJson(parameter))
                    .isEqualTo(TESTING_IDENTITY_CREDENTIAL.identity().orElseThrow());
            assertThat(queryParameters.get("key")).hasSize(1);
            assertThat(queryParameters.get("bucket")).hasSize(1);
        });
    }

    @Test
    public void testProxy404ResponseWhenRemoteS3ConnectionNotFound()
    {
        testingHttpRemoteS3ConnectionProviderServlet.setResponseStatusOverride(HttpStatus.NOT_FOUND);

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(internalClient::listBuckets)
                .extracting(S3Exception::statusCode)
                .isEqualTo(HttpStatus.NOT_FOUND.code());
    }

    @Test
    public void testProxy500ResponseWhenRemoteS3ConnectionProviderErrors()
    {
        testingHttpRemoteS3ConnectionProviderServlet.setResponseStatusOverride(HttpStatus.INTERNAL_SERVER_ERROR);

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(internalClient::listBuckets)
                .extracting(S3Exception::statusCode)
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    public void testProxy500ResponseWhenRemoteS3ConnectionProviderResponseMalformed()
    {
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {"malformed": "response"}
                """);

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(internalClient::listBuckets)
                .extracting(S3Exception::statusCode)
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    public void testProxy500ResponseWhenFacadeConfigsMalformed()
    {
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {
                    "remoteCredential": {
                        "accessKey": "%s",
                        "secretKey": "%s"
                    },
                    "remoteSessionRole": {
                        "region": "us-east-1",
                        "roleArn": "minio-doesnt-care"
                    },
                    "remoteS3FacadeConfiguration": {
                       "some": "config"
                    }
                }
                """.formatted(POLICY_USER_CREDENTIAL.accessKey(), POLICY_USER_CREDENTIAL.secretKey()));

        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(internalClient::listBuckets)
                .extracting(S3Exception::statusCode)
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.code());
    }
}
