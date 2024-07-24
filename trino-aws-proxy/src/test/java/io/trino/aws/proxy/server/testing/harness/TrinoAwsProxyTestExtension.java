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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.server.testing.ContainerS3Facade;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingS3ClientProvider;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactory;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreDestroyCallback;
import org.junit.jupiter.api.extension.TestInstantiationException;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerPlugin.assumedRoleProviderModule;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerPlugin.credentialsProviderModule;

public class TrinoAwsProxyTestExtension
        implements TestInstanceFactory, TestInstancePreDestroyCallback
{
    private final Map<String, TestingTrinoAwsProxyServer> testingServersRegistry = new ConcurrentHashMap<>();

    @Override
    public Object createTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext)
            throws TestInstantiationException
    {
        TrinoAwsProxyTest trinoAwsProxyTest = factoryContext.getTestClass().getAnnotation(TrinoAwsProxyTest.class);

        TestingTrinoAwsProxyServer.Builder builder = TestingTrinoAwsProxyServer.builder();

        List<BuilderFilter> filters = Stream.of(trinoAwsProxyTest.filters())
                .map(TrinoAwsProxyTestExtension::instantiateBuilderFilter)
                .collect(toImmutableList());
        for (BuilderFilter filter : filters) {
            builder = filter.filter(builder);
        }

        TestingTrinoAwsProxyServer trinoS3ProxyServer = builder
                .addModule(credentialsProviderModule("testing", TestingCredentialsRolesProvider.class, (binder) -> binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON)))
                .withProperty("credentials-provider.type", "testing")
                .addModule(assumedRoleProviderModule("testing", TestingCredentialsRolesProvider.class, (binder) -> binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON)))
                .withProperty("assumed-role-provider.type", "testing")
                .withS3Container()
                .addModule(binder -> {
                    binder.bind(S3Client.class).annotatedWith(ForS3Container.class).toProvider(S3Container.class);
                    newOptionalBinder(binder, Key.get(RemoteS3Facade.class, ForTesting.class))
                            .setDefault()
                            .to(ContainerS3Facade.PathStyleContainerS3Facade.class)
                            .asEagerSingleton();
                })
                .buildAndStart();
        testingServersRegistry.put(extensionContext.getUniqueId(), trinoS3ProxyServer);

        Injector injector = trinoS3ProxyServer.getInjector()
                .createChildInjector(binder -> {
                    binder.bind(TestingS3ClientProvider.TestingS3ClientConfig.class).in(Scopes.SINGLETON);
                    binder.bind(TestingS3ClientProvider.class).in(Scopes.SINGLETON);
                    binder.bind(S3Client.class).toProvider(TestingS3ClientProvider.class).in(Scopes.SINGLETON);
                    binder.bind(TestingTrinoAwsProxyServer.class).toInstance(trinoS3ProxyServer);
                    binder.bind(factoryContext.getTestClass()).in(Scopes.SINGLETON);
                });

        return injector.getInstance(factoryContext.getTestClass());
    }

    @Override
    public void preDestroyTestInstance(ExtensionContext context)
    {
        TestingTrinoAwsProxyServer trinoS3ProxyServer = testingServersRegistry.remove(context.getUniqueId());
        if (trinoS3ProxyServer != null) {
            trinoS3ProxyServer.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BuilderFilter instantiateBuilderFilter(Class builderFilterClass)
    {
        try {
            return (BuilderFilter) builderFilterClass.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not instantiate BuilderFilter: " + builderFilterClass.getName(), e);
        }
    }
}
