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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.glue.handler.GlueRequestHandler;
import io.trino.aws.proxy.glue.rest.ModelLoader;
import io.trino.aws.proxy.glue.rest.TrinoGlueResource;
import jakarta.ws.rs.WebApplicationException;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.aws.proxy.server.TrinoAwsProxyServerModule.bindResourceAtPath;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

public class TrinoGlueModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ModelLoader modelLoader = new ModelLoader();
        modelLoader.bindSerializers(binder);
        binder.bind(ModelLoader.class).toInstance(modelLoader);

        TrinoGlueConfig trinoGlueConfig = buildConfigObject(TrinoGlueConfig.class);
        bindResourceAtPath(jaxrsBinder(binder), TrinoGlueResource.class, trinoGlueConfig.getGluePath());

        newOptionalBinder(binder, GlueRequestHandler.class)
                .setDefault().toInstance((_, _, _) -> {
                    throw new WebApplicationException(NOT_FOUND);
                });
    }
}
