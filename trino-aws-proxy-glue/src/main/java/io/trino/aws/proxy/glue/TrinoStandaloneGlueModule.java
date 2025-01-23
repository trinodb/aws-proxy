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
package io.trino.aws.proxy.glue;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.server.credentials.CredentialsModule;
import io.trino.aws.proxy.server.rest.RestModule;
import io.trino.aws.proxy.server.signing.SigningModule;
import io.trino.aws.proxy.spi.remote.RemoteUriFacade;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

// intended for use-cases when the TrinoAwsProxyServer isn't desired
public class TrinoStandaloneGlueModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, RemoteUriFacade.class).setDefault().toInstance(_ -> {
            throw new UnsupportedOperationException();
        });

        install(new TrinoGlueModule());
        install(new SigningModule());
        install(new RestModule());
        install(new CredentialsModule());
    }
}
