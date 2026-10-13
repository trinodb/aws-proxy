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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.s3RequestRewriterModule;

public final class RequestRewriteUtil
{
    public static final String TEST_CREDENTIAL_REDIRECT_BUCKET = "redirected-bucket-for-credential";
    public static final String TEST_CREDENTIAL_REDIRECT_KEY = "redirected-key-for-credential";
    public static final Credential CREDENTIAL_TO_REDIRECT = new Credential("credential-to-redirect", UUID.randomUUID().toString());
    public static final String TAGGING_HEADER = "x-amz-tagging";
    public static final List<Tag> TEST_REWRITTEN_TAGS = ImmutableList.of(
            Tag.builder().key("rewritten-tag-1").value("rewritten-value-1").build(),
            Tag.builder().key("rewritten-tag-2").value("rewritten-value-2").build());
    public static final String TEST_REWRITE_PREFIX_QUERY_PARAM_BUCKET = "rewrite-prefix-query-param";
    public static final String TEST_REWRITTEN_PREFIX_QUERY_PARAM = "rewritten-prefix/";

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

        /**
         * If the credential is CREDENTIAL_TO_REDIRECT:
         * <ul>
         *   <li>Redirect to bucket TEST_CREDENTIAL_REDIRECT_BUCKET</li>
         *   <li>Redirect key (if any) to TEST_CREDENTIAL_REDIRECT_KEY</li>
         *   <li>Set "x-amz-tagging" header such that uploaded files (if applicable) have tags TEST_REWRITTEN_TAGS</li>
         *   <li>Only if the bucket in the incoming request is TEST_REWRITE_PREFIX_QUERY_PARAM_BUCKET, set "prefix" query parameter to value TEST_REWRITTEN_PREFIX_QUERY_PARAM</li>
         *   <li>Note that this means we should only run tests checking List operations (e.g., ListObjectsV2) against this bucket, as the query param doesn't make sense for other operations</li>
         * </ul>
         * Otherwise:
         * <ul>
         *   <li>Redirect all buckets to prepend "redirected-" in front</li>
         *   <li>Redirect all non-empty keys to prepend "redirected-" in front</li>
         *   <li>Empty keys are not changed</li>
         * </ul>
         */
        @Override
        public Optional<S3RewriteResult> testRewrite(String accessKey, String bucketName, String keyName, Optional<RequestHeaders> requestHeaders)
        {
            callCount.incrementAndGet();
            boolean redirectForTestCredential = accessKey.equalsIgnoreCase(CREDENTIAL_TO_REDIRECT.accessKey());
            if (redirectForTestCredential) {
                Optional<RequestHeaders> rewrittenRequestHeaders = requestHeaders
                        .flatMap(headers -> Optional.of(headers.withPassthroughHeaders(
                                ImmutableMultiMap
                                        .copyOfCaseInsensitive(headers.passthroughHeaders())
                                        .toBuilder()
                                        .putOrReplaceSingle(TAGGING_HEADER, getTaggingStringFromTags(TEST_REWRITTEN_TAGS))
                                        .build())));

                Optional<MultiMap> rewrittenQueryParams = Optional.empty();
                if (bucketName.equals(TEST_REWRITE_PREFIX_QUERY_PARAM_BUCKET)) {
                    rewrittenQueryParams = Optional.of(ImmutableMultiMap
                            .builder(false)
                            .putOrReplaceSingle("prefix", TEST_REWRITTEN_PREFIX_QUERY_PARAM)
                            .build());
                }

                return Optional.of(new S3RewriteResult(TEST_CREDENTIAL_REDIRECT_BUCKET, keyName.isEmpty() ? "" : TEST_CREDENTIAL_REDIRECT_KEY, rewrittenRequestHeaders, rewrittenQueryParams));
            }
            return Optional.of(new S3RewriteResult(getTargetName(bucketName), getTargetName(keyName), requestHeaders, Optional.empty()));
        }
    }

    private static String getTargetName(String name)
    {
        return name.isEmpty() ? "" : "redirected-%s".formatted(name);
    }

    public static String getTaggingStringFromTags(List<Tag> tags)
    {
        Map<String, List<String>> queryParams = tags
                .stream()
                .collect(Collectors.toMap(
                        Tag::key,
                        tag -> ImmutableList.of(tag.value()),
                        (list1, list2) -> Stream.concat(list1.stream(), list2.stream()).collect(Collectors.toList())));

        return SdkHttpUtils.encodeAndFlattenQueryParameters(queryParams).orElse("");
    }
}
