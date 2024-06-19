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
package io.trino.s3.proxy.server.testing;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.s3.proxy.server.TrinoS3ProxyModuleBuilder;
import io.trino.s3.proxy.server.TrinoS3ProxyServerModule;
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.spi.security.SecurityFacadeProvider;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class TestingTrinoS3ProxyServerModule
        extends TrinoS3ProxyServerModule
{
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForTestingRemoteCredentials {}

    @Override
    protected void moduleSpecificBinding(Binder binder)
    {
        Module module = TrinoS3ProxyModuleBuilder.builder()
                .withAssumedRoleProvider(binding -> binding.to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON))
                .withCredentialsProvider(binding -> binding.to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON))
                .build();
        binder.install(module);
        binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON);

        binder.bind(RemoteS3Facade.class).to(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);
        binder.bind(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, SecurityFacadeProvider.class).setDefault().to(TestingSecurityFacade.class).in(Scopes.SINGLETON);
        binder.bind(TestingSecurityFacade.class).in(Scopes.SINGLETON);
    }
}
