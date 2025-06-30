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
package io.trino.aws.proxy.server.remote.provider;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.testing.RequestRewriteUtil;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection.StaticRemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.trino.aws.proxy.server.testing.TestingUtil.LOREM_IPSUM;
import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_REMOTE_CREDENTIAL;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.remoteS3ConnectionProviderModule;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, RequestRewriteUtil.Filter.class, TestRemoteS3ConnectionProviderWithRewriter.Filter.class})
public class TestRemoteS3ConnectionProviderWithRewriter
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder
                    .withoutTestingRemoteS3ConnectionProvider()
                    .addModule(remoteS3ConnectionProviderModule("rewrite-test", DelegateRemoteS3ConnectionProvider.class,
                            binder -> binder.bind(DelegateRemoteS3ConnectionProvider.class).in(SINGLETON)))
                    .withProperty("remote-s3-connection-provider.type", "rewrite-test");
        }
    }

    public static class DelegateRemoteS3ConnectionProvider
            implements RemoteS3ConnectionProvider
    {
        private RemoteS3ConnectionProvider delegate;

        private final List<RemoteS3ConnectionProviderArgs> callArgs = new ArrayList<>();

        @Override
        public Optional<RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
        {
            callArgs.add(new RemoteS3ConnectionProviderArgs(signingMetadata, identity, request));
            return delegate.remoteConnection(signingMetadata, identity, request);
        }

        public void setDelegate(RemoteS3ConnectionProvider delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        public List<RemoteS3ConnectionProviderArgs> getCallArgs()
        {
            return callArgs;
        }

        public void reset()
        {
            callArgs.clear();
            delegate = null;
        }
    }

    public record RemoteS3ConnectionProviderArgs(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request) {}

    private final S3Client s3Client;
    private final S3Client storageClient;
    private final DelegateRemoteS3ConnectionProvider delegateRemoteS3ConnectionProvider;
    private final List<String> buckets;

    @Inject
    public TestRemoteS3ConnectionProviderWithRewriter(
            S3Client s3Client,
            @ForS3Container S3Client storageClient,
            DelegateRemoteS3ConnectionProvider delegateRemoteS3ConnectionProvider,
            @ForS3Container List<String> buckets)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        this.delegateRemoteS3ConnectionProvider = requireNonNull(delegateRemoteS3ConnectionProvider, "delegateRemoteS3ConnectionProvider is null");
        this.buckets = ImmutableList.copyOf(buckets);
    }

    @AfterEach
    public void cleanup()
    {
        delegateRemoteS3ConnectionProvider.reset();
    }

    @Test
    public void testRemoteS3ConnectionRetrievedWithRewrittenRequest()
            throws IOException
    {
        String bucket = buckets.getFirst();

        storageClient.putObject(PutObjectRequest.builder().bucket("redirected-" + bucket).key("redirected-test_key_1337").build(), RequestBody.fromString(LOREM_IPSUM));

        delegateRemoteS3ConnectionProvider.setDelegate((_, _, _) -> Optional.of(new StaticRemoteS3Connection(TESTING_REMOTE_CREDENTIAL)));

        ResponseInputStream<GetObjectResponse> resp = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key("test_key_1337").build());
        assertThat(resp.readAllBytes()).asString().isEqualTo(LOREM_IPSUM);

        assertThat(delegateRemoteS3ConnectionProvider.getCallArgs()).hasSize(1).first().satisfies(args -> {
            assertThat(args.request().bucketName()).isEqualTo("redirected-" + bucket);
            assertThat(args.request().keyInBucket()).isEqualTo("redirected-test_key_1337");
        });
    }
}
