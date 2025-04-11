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
package io.trino.aws.proxy.server.remote;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class DefaultRemoteS3Module
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DefaultRemoteS3Config.class);
        install(conditionalModule(
                DefaultRemoteS3Config.class,
                DefaultRemoteS3Config::getVirtualHostStyle,
                innerBinder -> newOptionalBinder(innerBinder, RemoteS3Facade.class)
                        .setBinding()
                        .to(VirtualHostStyleRemoteS3Facade.class)
                        .in(Scopes.SINGLETON),
                innerBinder -> newOptionalBinder(innerBinder, RemoteS3Facade.class)
                        .setBinding()
                        .to(PathStyleRemoteS3Facade.class)
                        .in(Scopes.SINGLETON)));
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForDefaultRemoteS3Facade {}
}
