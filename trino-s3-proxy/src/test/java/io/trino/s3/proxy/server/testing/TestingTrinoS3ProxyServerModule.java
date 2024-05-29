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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.s3.proxy.server.TrinoS3ProxyServerModule;
import io.trino.s3.proxy.server.credentials.AssumedRoleProvider;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.CredentialsProvider;
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.remote.RemoteSessionRole;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.UUID;

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
        binder.bind(CredentialsProvider.class).to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON);
        binder.bind(AssumedRoleProvider.class).to(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON);
        binder.bind(TestingCredentialsRolesProvider.class).in(Scopes.SINGLETON);

        binder.bind(RemoteS3Facade.class).to(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);
        binder.bind(TestingRemoteS3Facade.class).in(Scopes.SINGLETON);
    }

    @ForTestingRemoteCredentials
    @Provides
    @Singleton
    public Credentials provideRemoteCredentials(ManagedS3MockContainer s3MockContainer, TestingCredentialsRolesProvider credentialsController)
    {
        Credential policyUserCredential = s3MockContainer.policyUserCredential();

        RemoteSessionRole remoteSessionRole = new RemoteSessionRole("us-east-1", "minio-doesnt-care", Optional.empty());
        Credentials remoteCredentials = Credentials.build(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), policyUserCredential, remoteSessionRole);
        credentialsController.addCredentials(remoteCredentials);

        return remoteCredentials;
    }
}
