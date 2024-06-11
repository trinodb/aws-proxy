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
package io.trino.s3.proxy.hms;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.s3.proxy.spi.TrinoS3ProxyModuleBuilder;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacade;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacadeProvider;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class HmsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HmsConfig.class);

        binder.bind(TablesController.class).asEagerSingleton();

        install(TrinoS3ProxyModuleBuilder.builder()
                .withSecurityFacadeProvider(binding -> binding.to(InternalSecurityFacadeProvider.class))
                .build());

        newOptionalBinder(binder, HmsSecurityFacadeProvider.class).setDefault().toInstance((((_, _, _) -> HmsSecurityFacade.DEFAULT)));
    }
}