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
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteS3FacadeFactory;

import java.util.Map;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class DefaultRemoteS3FacadeFactory
        implements RemoteS3FacadeFactory
{
    @Override
    public RemoteS3Facade create(Map<String, String> configs)
    {
        return new Bootstrap(new DefaultRemoteS3Module())
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(configs)
                .initialize()
                .getInstance(RemoteS3Facade.class);
    }

    @Override
    public String name()
    {
        return "default";
    }

    private static class DefaultRemoteS3Module
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(DefaultRemoteS3Config.class);
            install(conditionalModule(
                    DefaultRemoteS3Config.class,
                    DefaultRemoteS3Config::getVirtualHostStyle,
                    innerBinder -> innerBinder.bind(RemoteS3Facade.class)
                            .to(VirtualHostStyleRemoteS3Facade.class)
                            .in(Scopes.SINGLETON),
                    innerBinder -> innerBinder.bind(RemoteS3Facade.class)
                            .to(PathStyleRemoteS3Facade.class)
                            .in(Scopes.SINGLETON)));
        }
    }
}
