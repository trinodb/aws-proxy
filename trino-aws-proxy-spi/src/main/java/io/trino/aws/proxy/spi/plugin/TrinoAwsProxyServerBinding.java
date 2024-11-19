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
package io.trino.aws.proxy.spi.plugin;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.plugin.config.AssumedRoleProviderConfig;
import io.trino.aws.proxy.spi.plugin.config.CredentialsProviderConfig;
import io.trino.aws.proxy.spi.plugin.config.PluginIdentifierConfig;
import io.trino.aws.proxy.spi.plugin.config.RemoteS3Config;
import io.trino.aws.proxy.spi.plugin.config.S3RequestRewriterConfig;
import io.trino.aws.proxy.spi.plugin.config.S3SecurityFacadeProviderConfig;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.rest.S3RequestRewriter;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public interface TrinoAwsProxyServerBinding
{
    Logger log = Logger.get(TrinoAwsProxyServerBinding.class);

    static Module credentialsProviderModule(String identifier, Class<? extends CredentialsProvider> implementationClass, Module module)
    {
        return optionalPluginModule(CredentialsProviderConfig.class, identifier, CredentialsProvider.class, implementationClass, module);
    }

    static Module assumedRoleProviderModule(String identifier, Class<? extends AssumedRoleProvider> implementationClass, Module module)
    {
        return optionalPluginModule(AssumedRoleProviderConfig.class, identifier, AssumedRoleProvider.class, implementationClass, module);
    }

    static Module s3SecurityFacadeProviderModule(String identifier, Class<? extends S3SecurityFacadeProvider> implementationClass, Module module)
    {
        return optionalPluginModule(S3SecurityFacadeProviderConfig.class, identifier, S3SecurityFacadeProvider.class, implementationClass, module);
    }

    static Module s3RequestRewriterModule(String identifier, Class<? extends S3RequestRewriter> implementationClass, Module module)
    {
        return optionalPluginModule(S3RequestRewriterConfig.class, identifier, S3RequestRewriter.class, implementationClass, module);
    }

    static Module remoteS3Module(String identifier, Class<? extends RemoteS3Facade> implementationClass, Module module)
    {
        return optionalPluginModule(RemoteS3Config.class, identifier, RemoteS3Facade.class, implementationClass, module);
    }

    static <T extends Identity> void bindIdentityType(Binder binder, Class<T> type)
    {
        newOptionalBinder(binder, new TypeLiteral<Class<? extends Identity>>() {}).setBinding().toProvider(() -> {
            log.info("Using %s identity type", type.getSimpleName());
            return type;
        });
    }

    static <Implementation> Module optionalPluginModule(
            Class<? extends PluginIdentifierConfig> configClass,
            String identifier,
            Class<Implementation> interfaceClass,
            Class<? extends Implementation> implementationClass,
            Module module)
    {
        return conditionalModule(configClass,
                config -> {
                    log.info("Registered %s plugin implementation %s with conditional identifier \"%s\"", interfaceClass.getSimpleName(), implementationClass.getSimpleName(), identifier);
                    return config.getPluginIdentifier().map(identifier::equals).orElse(false);
                },
                combine(
                        binder -> {
                            log.info("Using %s plugin implementation %s with identifier \"%s\"", interfaceClass.getSimpleName(), implementationClass.getSimpleName(), identifier);
                            newOptionalBinder(binder, interfaceClass).setBinding().to(implementationClass).in(Scopes.SINGLETON);
                        },
                        module));
    }
}
