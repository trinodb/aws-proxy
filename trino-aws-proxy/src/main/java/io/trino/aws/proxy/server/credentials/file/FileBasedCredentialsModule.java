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
package io.trino.aws.proxy.server.credentials.file;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.credentials.Identity;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.credentialsProviderModule;

public class FileBasedCredentialsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(credentialsProviderModule(
                "file",
                FileBasedCredentialsProvider.class,
                innerBinder -> {
                    configBinder(innerBinder).bindConfig(FileBasedCredentialsProviderConfig.class);
                    innerBinder.bind(FileBasedCredentialsProvider.class);
                }));
    }

    public static void bindFileBasedCredentialsIdentity(Binder binder, Class<? extends Identity> type)
    {
        newSetBinder(binder, Module.class).addBinding()
                .toInstance(new SimpleModule().addAbstractTypeMapping(Identity.class, type));
    }
}
