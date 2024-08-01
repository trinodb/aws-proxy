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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServerModule.ForTestingRemoteCredentials;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.MetastoreContainer;
import io.trino.aws.proxy.server.testing.containers.PostgresContainer;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer.PySparkV3Container;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer.PySparkV4Container;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.spi.credentials.Credentials;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_CREDENTIALS;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerPlugin.assumedRoleProviderModule;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerPlugin.credentialsProviderModule;

public final class TestingTrinoAwsProxyServer
        implements Closeable
{
    private final Injector injector;

    private TestingTrinoAwsProxyServer(Injector injector)
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
        private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        private boolean mockS3ContainerAdded;
        private boolean postgresContainerAdded;
        private boolean metastoreContainerAdded;
        private boolean v3PySparkContainerAdded;
        private boolean v4PySparkContainerAdded;
        private boolean addTestingCredentialsRoleProviders = true;

        public Builder addModule(Module module)
        {
            modules.add(module);
            return this;
        }

        public Builder withS3Container()
        {
            if (mockS3ContainerAdded) {
                return this;
            }
            mockS3ContainerAdded = true;

            modules.add(binder -> {
                binder.bind(TestingCredentialsInitializer.class).asEagerSingleton();

                binder.bind(S3Container.class).asEagerSingleton();
                binder.bind(Credentials.class).annotatedWith(ForTesting.class).toInstance(TESTING_CREDENTIALS);
                newOptionalBinder(binder, Key.get(new TypeLiteral<List<String>>() {}, ForS3Container.class)).setDefault().toInstance(ImmutableList.of());

                newOptionalBinder(binder, Key.get(RemoteS3Facade.class, ForTesting.class))
                        .setDefault()
                        .to(ContainerS3Facade.PathStyleContainerS3Facade.class)
                        .asEagerSingleton();
            });
            return this;
        }

        public Builder withProperty(String key, String value)
        {
            properties.put(key, value);
            return this;
        }

        public Builder withPostgresContainer()
        {
            if (postgresContainerAdded) {
                return this;
            }
            postgresContainerAdded = true;

            modules.add(binder -> binder.bind(PostgresContainer.class).asEagerSingleton());
            return this;
        }

        public Builder withMetastoreContainer()
        {
            // metastore requires postgres and S3
            withPostgresContainer();
            withS3Container();

            if (metastoreContainerAdded) {
                return this;
            }
            metastoreContainerAdded = true;

            modules.add(binder -> binder.bind(MetastoreContainer.class).asEagerSingleton());
            return this;
        }

        public Builder withV3PySparkContainer()
        {
            // pyspark requires metastore and S3
            withMetastoreContainer();
            withS3Container();

            if (v3PySparkContainerAdded) {
                return this;
            }
            v3PySparkContainerAdded = true;

            modules.add(binder -> binder.bind(PySparkContainer.class).to(PySparkV3Container.class).asEagerSingleton());
            return this;
        }

        public Builder withV4PySparkContainer()
        {
            // pyspark requires metastore and S3
            withMetastoreContainer();
            withS3Container();

            if (v4PySparkContainerAdded) {
                return this;
            }
            v4PySparkContainerAdded = true;

            modules.add(binder -> binder.bind(PySparkContainer.class).to(PySparkV4Container.class).asEagerSingleton());
            return this;
        }

        public Builder withServerHostName(String serverHostName)
        {
            properties.put("aws.proxy.hostname", serverHostName);
            return this;
        }

        public Builder withoutTestingCredentialsRoleProviders()
        {
            addTestingCredentialsRoleProviders = false;
            return this;
        }

        public TestingTrinoAwsProxyServer buildAndStart()
        {
            if (addTestingCredentialsRoleProviders) {
                addModule(credentialsProviderModule("testing", TestingCredentialsRolesProvider.class, (binder) -> binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON)));
                withProperty("credentials-provider.type", "testing");
                addModule(assumedRoleProviderModule("testing", TestingCredentialsRolesProvider.class, (binder) -> binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON)));
                withProperty("assumed-role-provider.type", "testing");

                modules.add(binder -> binder.bind(Credentials.class).annotatedWith(ForTestingRemoteCredentials.class).toProvider(TestingRemoteCredentialsProvider.class));
            }

            return start(modules.build(), properties.buildKeepingLast());
        }
    }

    static class TestingCredentialsInitializer
    {
        @Inject
        TestingCredentialsInitializer(TestingCredentialsRolesProvider credentialsController)
        {
            credentialsController.addCredentials(TESTING_CREDENTIALS);
        }
    }

    private static TestingTrinoAwsProxyServer start(Collection<Module> extraModules, Map<String, String> properties)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingTrinoAwsProxyServerModule())
                .add(new TestingNodeModule())
                .add(new EventModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        extraModules.forEach(modules::add);

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector = app.setOptionalConfigurationProperties(properties).initialize();
        Logging.initialize().setLevel("io.trino.aws.proxy", Level.DEBUG);

        return new TestingTrinoAwsProxyServer(injector);
    }
}
