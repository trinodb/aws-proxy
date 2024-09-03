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
package io.trino.aws.proxy.server.security.opa;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.security.opa.OpaClient;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.s3SecurityFacadeProviderModule;

public class OpaS3SecurityModule
        extends AbstractConfigurationAwareModule
{
    // set as config value for "s3-security.type"
    // user must bind an OpaS3SecurityMapper
    public static final String OPA_S3_SECURITY_IDENTIFIER = "opa";

    @Override
    protected void setup(Binder binder)
    {
        install(s3SecurityFacadeProviderModule(OPA_S3_SECURITY_IDENTIFIER, OpaS3SecurityFacadeProvider.class, internalBinder -> {
            configBinder(internalBinder).bindConfig(OpaS3SecurityConfig.class);
            httpClientBinder(internalBinder).bindHttpClient(OPA_S3_SECURITY_IDENTIFIER, ForOpa.class);
            newOptionalBinder(internalBinder, OpaClient.class).setDefault().to(DefaultOpaClient.class);
        }));
    }
}
