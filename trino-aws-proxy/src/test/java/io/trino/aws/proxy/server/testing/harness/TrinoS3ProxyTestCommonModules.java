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
import com.google.inject.multibindings.OptionalBinder;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.server.testing.ContainerS3Facade;
import io.trino.aws.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;

import java.util.List;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.aws.proxy.server.testing.TestingUtil.LOCALHOST_DOMAIN;

public final class TrinoS3ProxyTestCommonModules
{
    public static class WithConfiguredBuckets
            implements BuilderFilter
    {
        private static final List<String> CONFIGURED_BUCKETS = ImmutableList.of("one", "two", "three");

        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.addModule(binder ->
                    OptionalBinder.newOptionalBinder(binder, Key.get(new TypeLiteral<List<String>>() {}, ForS3Container.class))
                            .setBinding()
                            .toInstance(CONFIGURED_BUCKETS));
        }
    }

    public static class WithVirtualHostAddressing
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.addModule(binder ->
                    OptionalBinder.newOptionalBinder(binder, Key.get(RemoteS3Facade.class, ForTesting.class))
                            .setBinding()
                            .to(ContainerS3Facade.VirtualHostStyleContainerS3Facade.class)
                            .asEagerSingleton());
        }
    }

    public static class WithVirtualHostEnabledProxy
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.withProperty("s3proxy.s3.hostname", LOCALHOST_DOMAIN);
        }
    }

    public static class WithTestingHttpClient
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.addModule(binder -> httpClientBinder(binder).bindHttpClient("testing", ForTesting.class));
        }
    }

    public static class WithAllContainers
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.withPySparkContainer()
                    .withS3Container()
                    .withPostgresContainer()
                    .withMetastoreContainer();
        }
    }

    private TrinoS3ProxyTestCommonModules() {}
}
