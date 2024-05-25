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

import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.s3.proxy.server.credentials.Credentials.Credential;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer;
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import io.trino.s3.proxy.server.testing.TestingS3ClientProvider;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactory;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreDestroyCallback;
import org.junit.jupiter.api.extension.TestInstantiationException;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TrinoS3ProxyTestExtension
        implements TestInstanceFactory, TestInstancePreDestroyCallback
{
    private final Map<String, TestingTrinoS3ProxyServer> testingServersRegistry = new ConcurrentHashMap<>();

    @Override
    public Object createTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext)
            throws TestInstantiationException
    {
        TrinoS3ProxyTest trinoS3ProxyTest = factoryContext.getTestClass().getAnnotation(TrinoS3ProxyTest.class);

        TestingTrinoS3ProxyServer.Builder builder = TestingTrinoS3ProxyServer.builder();

        Stream.of(trinoS3ProxyTest.modules())
                .map(TrinoS3ProxyTestExtension::instantiateModule)
                .forEach(builder::addModule);

        List<BuilderFilter> filters = Stream.of(trinoS3ProxyTest.filters())
                .map(TrinoS3ProxyTestExtension::instantiateBuilderFilter)
                .collect(toImmutableList());
        for (BuilderFilter filter : filters) {
            builder = filter.filter(builder);
        }

        TestingTrinoS3ProxyServer trinoS3ProxyServer = builder
                .buildAndStart();
        testingServersRegistry.put(extensionContext.getUniqueId(), trinoS3ProxyServer);

        Credential containerCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Injector injector = trinoS3ProxyServer.getInjector()
                .createChildInjector(binder -> {
                    binder.bind(String.class).annotatedWith(ForS3MockContainer.class).toInstance(trinoS3ProxyTest.initialBuckets());
                    binder.bind(Credential.class).annotatedWith(ForS3MockContainer.class).toInstance(containerCredential);
                    binder.bind(ManagedS3MockContainer.class).asEagerSingleton();
                    binder.bind(S3Client.class).toProvider(TestingS3ClientProvider.class).in(Scopes.SINGLETON);
                    binder.bind(TestingTrinoS3ProxyServer.class).toInstance(trinoS3ProxyServer);
                    binder.bind(factoryContext.getTestClass()).in(Scopes.SINGLETON);
                });

        return injector.getInstance(factoryContext.getTestClass());
    }

    @Override
    public void preDestroyTestInstance(ExtensionContext context)
    {
        TestingTrinoS3ProxyServer trinoS3ProxyServer = testingServersRegistry.remove(context.getUniqueId());
        if (trinoS3ProxyServer != null) {
            trinoS3ProxyServer.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Module instantiateModule(Class moduleClass)
    {
        try {
            return (Module) moduleClass.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not instantiate module: " + moduleClass.getName(), e);
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
