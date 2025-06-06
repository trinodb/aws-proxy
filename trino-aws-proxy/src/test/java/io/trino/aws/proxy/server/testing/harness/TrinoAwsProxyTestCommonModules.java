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
package io.trino.aws.proxy.server.testing.harness;

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;

import java.util.List;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.aws.proxy.server.testing.TestingUtil.LOCALHOST_DOMAIN;

public final class TrinoAwsProxyTestCommonModules
{
    public static class WithConfiguredBuckets
            implements BuilderFilter
    {
        private static final List<String> CONFIGURED_BUCKETS = ImmutableList.of("one", "two", "three");

        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.addModule(binder ->
                    newOptionalBinder(binder, Key.get(new TypeLiteral<List<String>>() {}, ForS3Container.class))
                            .setBinding()
                            .toInstance(CONFIGURED_BUCKETS));
        }
    }

    public static class WithVirtualHostEnabledProxy
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withProperty("aws.proxy.s3.hostname", LOCALHOST_DOMAIN);
        }
    }

    public static class WithTestingHttpClient
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.addModule(binder -> httpClientBinder(binder).bindHttpClient("testing", ForTesting.class));
        }
    }

    private TrinoAwsProxyTestCommonModules() {}
}
