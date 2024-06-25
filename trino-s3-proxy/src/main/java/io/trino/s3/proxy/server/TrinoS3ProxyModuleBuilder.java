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
package io.trino.s3.proxy.server;

import com.google.inject.Module;
import com.google.inject.binder.LinkedBindingBuilder;
import io.trino.s3.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.s3.proxy.spi.credentials.CredentialsProvider;
import io.trino.s3.proxy.spi.security.SecurityFacadeProvider;

import java.util.Optional;
import java.util.function.Consumer;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public interface TrinoS3ProxyModuleBuilder
{
    static Builder builder()
    {
        return new Builder();
    }

    final class Builder
    {
        private Optional<Consumer<LinkedBindingBuilder<CredentialsProvider>>> credentialsProviderBinder = Optional.empty();
        private Optional<Consumer<LinkedBindingBuilder<AssumedRoleProvider>>> assumedRoleProviderBinder = Optional.empty();
        private Optional<Consumer<LinkedBindingBuilder<SecurityFacadeProvider>>> securityFacadeProviderBinder = Optional.empty();

        private Builder() {}

        public Builder withCredentialsProvider(Consumer<LinkedBindingBuilder<CredentialsProvider>> credentialsProviderBinder)
        {
            this.credentialsProviderBinder = Optional.of(credentialsProviderBinder);
            return this;
        }

        public Builder withAssumedRoleProvider(Consumer<LinkedBindingBuilder<AssumedRoleProvider>> assumedRoleProviderBinder)
        {
            this.assumedRoleProviderBinder = Optional.of(assumedRoleProviderBinder);
            return this;
        }

        public Builder withSecurityFacadeProvider(Consumer<LinkedBindingBuilder<SecurityFacadeProvider>> securityFacadeProviderBinder)
        {
            this.securityFacadeProviderBinder = Optional.of(securityFacadeProviderBinder);
            return this;
        }

        public Module build()
        {
            return binder -> {
                credentialsProviderBinder.ifPresent(binding -> binding.accept(newOptionalBinder(binder, CredentialsProvider.class).setBinding()));
                assumedRoleProviderBinder.ifPresent(binding -> binding.accept(newOptionalBinder(binder, AssumedRoleProvider.class).setBinding()));
                securityFacadeProviderBinder.ifPresent(binding -> binding.accept(newOptionalBinder(binder, SecurityFacadeProvider.class).setBinding()));
            };
        }
    }
}
