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

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.RequestRewriteUtil;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriter;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.CREDENTIAL_TO_REDIRECT;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_BUCKET;
import static io.trino.aws.proxy.server.testing.RequestRewriteUtil.TEST_CREDENTIAL_REDIRECT_KEY;
import static io.trino.aws.proxy.server.testing.TestingUtil.TEST_FILE;
import static io.trino.aws.proxy.server.testing.TestingUtil.assertFileNotInS3;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, TestSecurityControllerCalledBeforeRewrite.Filter.class, RequestRewriteUtil.Filter.class})
public class TestSecurityControllerCalledBeforeRewrite
{
    private final S3Client storageClient;
    private final TestingS3RequestRewriter s3requestRewriter;

    private final URI baseUri;
    private final String relativePath;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.addModule(binder -> newOptionalBinder(binder, S3SecurityFacadeProvider.class).setBinding().toInstance((_, _) -> _ -> SecurityResponse.FAILURE));
        }
    }

    @Inject
    public TestSecurityControllerCalledBeforeRewrite(
            @ForS3Container S3Client storageClient,
            TestingS3RequestRewriter s3RequestRewriter,
            TestingHttpServer testingHttpServer,
            TrinoAwsProxyConfig config)
    {
        this.storageClient = requireNonNull(storageClient, "remoteClient is null");
        this.s3requestRewriter = requireNonNull(s3RequestRewriter, "requestRewriteController is null");
        this.baseUri = testingHttpServer.getBaseUrl();
        this.relativePath = config.getS3Path();
    }

    @Test
    public void testSecurityControllerCalledBeforeRewrite()
    {
        String testBucket = "dummy";
        String testKey = "dummy-key";
        try (S3Client testS3Client = clientBuilder(baseUri, Optional.of(relativePath))
                .credentialsProvider(() -> AwsBasicCredentials.create(CREDENTIAL_TO_REDIRECT.accessKey(), CREDENTIAL_TO_REDIRECT.secretKey()))
                .build()) {
            PutObjectRequest uploadRequest = PutObjectRequest.builder().bucket(testBucket).key(testKey).build();

            assertThatThrownBy(() -> testS3Client.putObject(uploadRequest, TEST_FILE))
                    .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                    .extracting(S3Exception::statusCode)
                    .isEqualTo(401);
        }
        assertEquals(0, ((RequestRewriteUtil.Rewriter) s3requestRewriter).getCallCount());
        assertFileNotInS3(storageClient, TEST_CREDENTIAL_REDIRECT_BUCKET, TEST_CREDENTIAL_REDIRECT_KEY);
        assertFileNotInS3(storageClient, testBucket, testKey);
    }
}
