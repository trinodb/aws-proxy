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

import com.google.common.collect.ImmutableList;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.AbstractTestProxiedRequests;
import io.trino.aws.proxy.server.remote.provider.http.TestHttpRemoteS3ConnectionProviderWithCustomFacade.WithSecondaryS3Container;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.TestingUtil;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.UUID;

import static io.trino.aws.proxy.server.testing.TestingUtil.LOREM_IPSUM;
import static io.trino.aws.proxy.server.testing.TestingUtil.assertFileNotInS3;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, WithHttpRemoteS3ConnectionProvider.class, WithSecondaryS3Container.class})
public class TestHttpRemoteS3ConnectionProviderWithCustomFacade
        extends AbstractTestProxiedRequests
{
    public static final class WithSecondaryS3Container
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            Credential secondaryRemoteCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            S3Container secondaryS3Container = new S3Container(ImmutableList.of(), secondaryRemoteCredential);
            return builder.addModule(binder -> {
                binder.bind(S3Container.class).annotatedWith(ForSecondaryS3Container.class).toInstance(secondaryS3Container);
                binder.bind(S3Client.class).annotatedWith(ForSecondaryS3Container.class).toInstance(secondaryS3Container.get());
                binder.bind(Credential.class).annotatedWith(ForSecondaryS3Container.class).toInstance(secondaryRemoteCredential);
            });
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForSecondaryS3Container
    {
    }

    private final S3Client internalClient;
    private final S3Client remoteClient;
    private final Credential originalRemoteCredential;
    private final List<String> initialBuckets;
    private final S3Container secondaryS3Container;
    private final S3Client secondaryRemoteClient;
    private final Credential secondaryRemoteCredential;
    private final TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet;
    private final HttpRemoteS3ConnectionProvider httpRemoteS3ConnectionProvider;

    @Inject
    public TestHttpRemoteS3ConnectionProviderWithCustomFacade(S3Client internalClient,
            @ForS3Container S3Client remoteClient,
            TestingS3RequestRewriteController requestRewriteController,
            @ForS3Container Credential originalRemoteCredential,
            @ForS3Container List<String> initialBuckets,
            @ForSecondaryS3Container S3Container secondaryS3Container,
            @ForSecondaryS3Container S3Client secondaryRemoteClient,
            @ForSecondaryS3Container Credential secondaryRemoteCredential,
            TestingHttpRemoteS3ConnectionProviderServlet testingHttpRemoteS3ConnectionProviderServlet,
            RemoteS3ConnectionProvider httpRemoteS3ConnectionProvider)
    {
        super(internalClient, secondaryRemoteClient, requestRewriteController);

        this.internalClient = requireNonNull(internalClient, "internalClient is null");
        this.remoteClient = requireNonNull(remoteClient, "remoteClient is null");
        this.originalRemoteCredential = requireNonNull(originalRemoteCredential, "originalCredential is null");
        this.initialBuckets = ImmutableList.copyOf(initialBuckets);
        this.secondaryS3Container = requireNonNull(secondaryS3Container, "secondaryS3Container is null");
        this.secondaryRemoteClient = requireNonNull(secondaryRemoteClient, " secondaryRemoteClient is null");
        this.secondaryRemoteCredential = requireNonNull(secondaryRemoteCredential, "secondaryRemoteCredential is null");
        this.testingHttpRemoteS3ConnectionProviderServlet = requireNonNull(testingHttpRemoteS3ConnectionProviderServlet, "testingHttpRemoteS3ConnectionProviderServlet is null");
        this.httpRemoteS3ConnectionProvider = (HttpRemoteS3ConnectionProvider) requireNonNull(httpRemoteS3ConnectionProvider, "httpRemoteS3ConnectionProvider is null");

        for (String bucket : this.initialBuckets) {
            secondaryRemoteClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    @BeforeEach
    public void beforeEach()
    {
        testingHttpRemoteS3ConnectionProviderServlet.reset();
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {
                    "remoteCredential": {
                        "accessKey": "%s",
                        "secretKey": "%s"
                    },
                    "remoteS3FacadeConfiguration": {
                       "remoteS3.https": false,
                       "remoteS3.domain": "%s",
                       "remoteS3.port": "%s",
                       "remoteS3.virtual-host-style": false,
                       "remoteS3.hostname.template": "${domain}"
                    }
                }
                """.formatted(secondaryRemoteCredential.accessKey(), secondaryRemoteCredential.secretKey(), secondaryS3Container.containerHost().getHost(),
                secondaryS3Container.containerHost().getPort()));
        httpRemoteS3ConnectionProvider.resetCache();
    }

    @Test
    public void testPutObjectInBothS3Containers()
            throws IOException
    {
        String bucket = initialBuckets.getFirst();
        String objectKey = UUID.randomUUID().toString();

        // When we upload an object to the proxy, it will first go to the secondary container (see bforeEaech setting the facade configuration)
        internalClient.putObject(PutObjectRequest.builder().bucket(bucket).key(objectKey).build(), RequestBody.fromString(LOREM_IPSUM));

        // Then files is in secondary S3 container
        assertThat(getFileFromStorage(secondaryRemoteClient, bucket, objectKey)).isEqualTo(LOREM_IPSUM);
        // Files not in first S3 container
        assertFileNotInS3(remoteClient, bucket, objectKey);

        // Update remoteS3Connection to not override remoteS3FacadeConfig
        testingHttpRemoteS3ConnectionProviderServlet.setResponse("""
                {
                    "remoteCredential": {
                        "accessKey": "%s",
                        "secretKey": "%s"
                    }
                }
                """.formatted(originalRemoteCredential.accessKey(), originalRemoteCredential.secretKey()));
        httpRemoteS3ConnectionProvider.resetCache();

        // File is not in aws-proxy
        assertFileNotInS3(internalClient, bucket, objectKey);
        // Put file again
        internalClient.putObject(PutObjectRequest.builder().bucket(bucket).key(objectKey).build(), RequestBody.fromString(LOREM_IPSUM));
        // Files is in first remote S3 container
        assertThat(getFileFromStorage(remoteClient, bucket, objectKey)).isEqualTo(LOREM_IPSUM);

        TestingUtil.cleanupBuckets(remoteClient);
        TestingUtil.cleanupBuckets(secondaryRemoteClient);
    }
}
