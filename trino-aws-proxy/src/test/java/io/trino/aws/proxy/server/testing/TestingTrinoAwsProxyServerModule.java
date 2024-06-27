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
package io.trino.aws.proxy.server.testing;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.TrinoAwsProxyServerModule;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.server.security.S3SecurityController;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static io.trino.aws.proxy.spi.TrinoAwsProxyBinder.trinoAwsProxyBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class TestingTrinoAwsProxyServerModule
        extends TrinoAwsProxyServerModule
{
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForTestingRemoteCredentials {}

    @Override
    protected void moduleSpecificBinding(Binder binder)
    {
        trinoAwsProxyBinder(binder)
                .bindAssumedRoleProvider(binding -> binding.to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON))
                .bindCredentialsProvider(binding -> binding.to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON));
        binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON);

        binder.bind(RemoteS3Facade.class).to(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);
        binder.bind(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);

        binder.bind(S3SecurityController.class).to(TestingS3SecurityController.class).in(Scopes.SINGLETON);
        binder.bind(TestingS3SecurityController.class).in(Scopes.SINGLETON);
    }
}
