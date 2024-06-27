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
package io.trino.aws.proxy.spi;

import com.google.inject.Binder;
import com.google.inject.binder.LinkedBindingBuilder;
import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;

import java.util.function.Consumer;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public interface TrinoAwsProxyBinder
{
    static TrinoAwsProxyBinder.InternalBinder trinoAwsProxyBinder(Binder binder)
    {
        return new InternalBinder(binder);
    }

    TrinoAwsProxyBinder bindCredentialsProvider(Consumer<LinkedBindingBuilder<CredentialsProvider>> credentialsProviderBinder);

    TrinoAwsProxyBinder bindAssumedRoleProvider(Consumer<LinkedBindingBuilder<AssumedRoleProvider>> assumedRoleProviderBinder);

    TrinoAwsProxyBinder bindS3SecurityFacadeProvider(Consumer<LinkedBindingBuilder<S3SecurityFacadeProvider>> securityFacadeProviderBinder);

    final class InternalBinder
            implements TrinoAwsProxyBinder
    {
        private final Binder binder;

        private InternalBinder(Binder binder)
        {
            this.binder = requireNonNull(binder, "binder is null");
        }

        @Override
        public TrinoAwsProxyBinder bindCredentialsProvider(Consumer<LinkedBindingBuilder<CredentialsProvider>> credentialsProviderBinder)
        {
            credentialsProviderBinder.accept(newOptionalBinder(binder, CredentialsProvider.class).setBinding());
            return this;
        }

        @Override
        public TrinoAwsProxyBinder bindAssumedRoleProvider(Consumer<LinkedBindingBuilder<AssumedRoleProvider>> assumedRoleProviderBinder)
        {
            assumedRoleProviderBinder.accept(newOptionalBinder(binder, AssumedRoleProvider.class).setBinding());
            return this;
        }

        @Override
        public TrinoAwsProxyBinder bindS3SecurityFacadeProvider(Consumer<LinkedBindingBuilder<S3SecurityFacadeProvider>> securityFacadeProviderBinder)
        {
            securityFacadeProviderBinder.accept(newOptionalBinder(binder, S3SecurityFacadeProvider.class).setBinding());
            return this;
        }
    }
}
