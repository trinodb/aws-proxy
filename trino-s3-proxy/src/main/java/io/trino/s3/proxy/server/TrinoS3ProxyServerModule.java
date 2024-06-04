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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.s3.proxy.server.credentials.AssumedRoleProvider;
import io.trino.s3.proxy.server.credentials.CredentialsController;
import io.trino.s3.proxy.server.credentials.CredentialsProvider;
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningControllerConfig;
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.remote.VirtualHostStyleRemoteS3Facade;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyClient;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyClient.ForProxyClient;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyResource;
import io.trino.s3.proxy.server.rest.TrinoStsResource;
import io.trino.s3.proxy.server.security.SecurityController;
import io.trino.s3.proxy.server.security.SecurityFacadeProvider;
import io.trino.s3.proxy.server.security.SecurityResponse;

import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class TrinoS3ProxyServerModule
        implements Module
{
    @Override
    public final void configure(Binder binder)
    {
        jaxrsBinder(binder).bind(TrinoS3ProxyResource.class);
        jaxrsBinder(binder).bind(TrinoStsResource.class);

        configBinder(binder).bindConfig(SigningControllerConfig.class);
        configBinder(binder).bindConfig(TrinoS3ProxyConfig.class);
        binder.bind(SigningController.class).in(Scopes.SINGLETON);
        binder.bind(CredentialsController.class).in(Scopes.SINGLETON);
        binder.bind(SecurityController.class).in(Scopes.SINGLETON);

        // TODO config, etc.
        httpClientBinder(binder).bindHttpClient("ProxyClient", ForProxyClient.class);
        binder.bind(TrinoS3ProxyClient.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, CredentialsProvider.class).setDefault().toInstance((_, _) -> Optional.empty());
        newOptionalBinder(binder, AssumedRoleProvider.class).setDefault().toInstance((_, _, _, _, _, _) -> Optional.empty());

        moduleSpecificBinding(binder);
    }

    protected void moduleSpecificBinding(Binder binder)
    {
        newOptionalBinder(binder, SecurityFacadeProvider.class).setDefault().toInstance((_, _, _) -> (_, _) -> SecurityResponse.DEFAULT);
        binder.bind(RemoteS3Facade.class).to(VirtualHostStyleRemoteS3Facade.class).in(Scopes.SINGLETON);
    }
}
