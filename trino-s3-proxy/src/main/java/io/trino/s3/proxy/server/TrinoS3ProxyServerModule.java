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
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningControllerConfig;
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.remote.VirtualHostStyleRemoteS3Facade;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyClient;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyClient.ForProxyClient;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyResource;
import io.trino.s3.proxy.server.rest.TrinoStsResource;

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
        binder.bind(SigningController.class).in(Scopes.SINGLETON);

        // TODO config, etc.
        httpClientBinder(binder).bindHttpClient("ProxyClient", ForProxyClient.class);
        binder.bind(TrinoS3ProxyClient.class).in(Scopes.SINGLETON);

        moduleSpecificBinding(binder);
    }

    protected void moduleSpecificBinding(Binder binder)
    {
        binder.bind(RemoteS3Facade.class).to(VirtualHostStyleRemoteS3Facade.class).in(Scopes.SINGLETON);
    }
}
