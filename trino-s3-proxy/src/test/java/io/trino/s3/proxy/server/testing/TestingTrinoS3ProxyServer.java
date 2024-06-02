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
package io.trino.s3.proxy.server.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.s3.proxy.server.credentials.Credentials;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.s3.proxy.server.testing.TestingUtil.TESTING_CREDENTIALS;

public final class TestingTrinoS3ProxyServer
        implements Closeable
{
    private final Injector injector;

    private TestingTrinoS3ProxyServer(Injector injector)
    {
        this.injector = injector;
    }

    @Override
    public void close()
    {
        injector.getInstance(LifeCycleManager.class).stop();
    }

    public Injector getInjector()
    {
        return injector;
    }

    public TestingCredentialsRolesProvider getCredentialsController()
    {
        return injector.getInstance(TestingCredentialsRolesProvider.class);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableSet.Builder<Module> modules = ImmutableSet.builder();

        private Builder()
        {
        }

        public Builder addModule(Module module)
        {
            this.modules.add(module);
            return this;
        }

        public Builder withMockS3Container()
        {
            this.modules.add(binder -> {
                binder.bind(ManagedS3MockContainer.class).asEagerSingleton();
                binder.bind(Credentials.class).annotatedWith(TestingUtil.ForTesting.class).toInstance(TESTING_CREDENTIALS);
                newOptionalBinder(binder, Key.get(new TypeLiteral<List<String>>(){}, ManagedS3MockContainer.ForS3MockContainer.class)).setDefault().toInstance(ImmutableList.of());
            });
            return this;
        }

        public TestingTrinoS3ProxyServer buildAndStart()
        {
            return start(modules.build());
        }
    }

    private static TestingTrinoS3ProxyServer start(Collection<Module> extraModules)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingTrinoS3ProxyServerModule())
                .add(new TestingNodeModule())
                .add(new EventModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        extraModules.forEach(modules::add);

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector = app.initialize();
        return new TestingTrinoS3ProxyServer(injector);
    }
}
