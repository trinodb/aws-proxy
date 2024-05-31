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
package io.trino.s3.proxy.server.testing.harness;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.OptionalBinder;
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.testing.ContainerS3Facade;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import io.trino.s3.proxy.server.testing.TestingConstants.ForTesting;

import java.util.List;

public final class TrinoS3ProxyTestCommonModules
{
    public static final class WithConfiguredBuckets
            implements Module
    {
        private static final List<String> CONFIGURED_BUCKETS = ImmutableList.of("one", "two", "three");

        @Override
        public void configure(Binder binder)
        {
            OptionalBinder.newOptionalBinder(binder, Key.get(new TypeLiteral<List<String>>() {}, ForS3MockContainer.class))
                    .setBinding()
                    .toInstance(CONFIGURED_BUCKETS);
        }
    }

    public static final class WithVirtualHostAddressing
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            OptionalBinder.newOptionalBinder(binder, Key.get(RemoteS3Facade.class, ForTesting.class))
                    .setBinding()
                    .to(ContainerS3Facade.VirtualHostStyleContainerS3Facade.class)
                    .asEagerSingleton();
        }
    }

    private TrinoS3ProxyTestCommonModules() {}
}
