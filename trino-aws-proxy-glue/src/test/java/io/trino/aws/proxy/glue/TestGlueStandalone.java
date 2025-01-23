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
package io.trino.aws.proxy.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Map;

import static io.trino.aws.proxy.glue.TestingCredentialsProvider.CREDENTIALS;
import static io.trino.aws.proxy.glue.handler.GlueRequestHandlerBinding.glueRequestHandlerBinding;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.credentialsProviderModule;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGlueStandalone
        extends TestGlueBase<TestGlueStandalone.StandaloneContext>
{
    public record StandaloneContext(URI baseUrl, LifeCycleManager lifeCycleManager)
            implements TestingGlueContext
    {
        public StandaloneContext
        {
            requireNonNull(baseUrl, "baseUrl is null");
            requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        }
    }

    public TestGlueStandalone()
    {
        super(new TrinoGlueConfig(), CREDENTIALS, buildContext());
    }

    @AfterAll
    public void shutdown()
    {
        super.shutdown();

        context().lifeCycleManager().stop();
    }

    private static StandaloneContext buildContext()
    {
        Module module = new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                install(credentialsProviderModule("testing", TestingCredentialsProvider.class, (subBinder) -> subBinder.bind(TestingCredentialsProvider.class).in(Scopes.SINGLETON)));
                glueRequestHandlerBinding(binder).bind(binding -> binding.to(TestingGlueRequestHandler.class));
            }
        };

        Map<String, String> properties = ImmutableMap.of(
                "credentials-provider.type", "testing",
                "assumed-role-provider.type", "testing");

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(module)
                .add(new TrinoStandaloneGlueModule())
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule());

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector = app.setOptionalConfigurationProperties(properties).initialize();

        TestingHttpServer httpServer = injector.getInstance(TestingHttpServer.class);
        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        return new StandaloneContext(httpServer.getBaseUrl(), lifeCycleManager);
    }
}
