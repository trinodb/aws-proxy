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
package io.trino.aws.proxy.server.credentials;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.credentials.StandardIdentity;
import io.trino.aws.proxy.spi.plugin.config.AssumedRoleProviderConfig;
import io.trino.aws.proxy.spi.plugin.config.CredentialsProviderConfig;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class CredentialsModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(CredentialsModule.class);

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(CredentialsController.class).in(Scopes.SINGLETON);

        // CredentialsProvider binder
        configBinder(binder).bindConfig(CredentialsProviderConfig.class);
        newOptionalBinder(binder, CredentialsProvider.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", CredentialsProvider.class.getSimpleName());
            return CredentialsProvider.NOOP;
        });
        newOptionalBinder(binder, new TypeLiteral<Class<? extends Identity>>() {}).setDefault().toProvider(() -> {
            log.info("Using %s identity type", StandardIdentity.class.getSimpleName());
            return StandardIdentity.class;
        });
        newSetBinder(binder, com.fasterxml.jackson.databind.Module.class).addBinding().toProvider(JsonIdentityProvider.class).in(Scopes.SINGLETON);

        // AssumedRoleProvider binder
        configBinder(binder).bindConfig(AssumedRoleProviderConfig.class);
        // AssumedRoleProvider provided implementations
        newOptionalBinder(binder, AssumedRoleProvider.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", AssumedRoleProvider.class.getSimpleName());
            return AssumedRoleProvider.NOOP;
        });
    }
}
