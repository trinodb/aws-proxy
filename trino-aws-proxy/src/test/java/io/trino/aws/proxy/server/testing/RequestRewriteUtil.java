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
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.s3RequestRewriterModule;

public final class RequestRewriteUtil
{
    public static final String TEST_CREDENTIAL_REDIRECT_BUCKET = "redirected-bucket-for-credential";
    public static final String TEST_CREDENTIAL_REDIRECT_KEY = "redirected-key-for-credential";
    public static final Credential CREDENTIAL_TO_REDIRECT = new Credential("credential-to-redirect", UUID.randomUUID().toString());

    private RequestRewriteUtil() {}

    public static class SetupRequestRewrites
    {
        @Inject
        public SetupRequestRewrites(
                TestingCredentialsRolesProvider credentialsRolesProvider,
                @ForS3Container List<String> configuredBuckets,
                @ForS3Container S3Client storageClient)
        {
            credentialsRolesProvider.addCredentials(new IdentityCredential(CREDENTIAL_TO_REDIRECT));
            configuredBuckets.forEach(bucket -> storageClient.createBucket(r -> r.bucket(getTargetName(bucket))));
            storageClient.createBucket(r -> r.bucket(TEST_CREDENTIAL_REDIRECT_BUCKET));
        }
    }

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder
                    .addModule(s3RequestRewriterModule("testing", TestingS3RequestRewriter.class, binder -> {
                        newOptionalBinder(binder, TestingS3RequestRewriter.class).setBinding().to(Rewriter.class).in(Scopes.SINGLETON);
                        binder.bind(SetupRequestRewrites.class).asEagerSingleton();
                    }))
                    .withProperty("s3-request-rewriter.type", "testing");
        }
    }

    public static class Rewriter
            implements TestingS3RequestRewriter
    {
        private final AtomicInteger callCount = new AtomicInteger();

        public int getCallCount()
        {
            return callCount.get();
        }

        @Override
        public Optional<S3RewriteResult> testRewrite(String accessKey, String bucketName, String keyName)
        {
            callCount.incrementAndGet();
            boolean redirectForTestCredential = accessKey.equalsIgnoreCase(CREDENTIAL_TO_REDIRECT.accessKey());
            if (redirectForTestCredential) {
                return Optional.of(new S3RewriteResult(TEST_CREDENTIAL_REDIRECT_BUCKET, keyName.isEmpty() ? "" : TEST_CREDENTIAL_REDIRECT_KEY));
            }
            return Optional.of(new S3RewriteResult(getTargetName(bucketName), getTargetName(keyName)));
        }
    }

    private static String getTargetName(String name)
    {
        return name.isEmpty() ? "" : "redirected-%s".formatted(name);
    }
}
