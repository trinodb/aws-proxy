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
package io.trino.aws.proxy.server.credentials.http;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.credentials.Credentials;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.credentialsProviderModule;

public class HttpCredentialsModule
        extends AbstractConfigurationAwareModule
{
    // set as config value for "credentials-provider.type"
    public static final String HTTP_CREDENTIALS_PROVIDER_IDENTIFIER = "http";
    public static final String HTTP_CREDENTIALS_PROVIDER_HTTP_CLIENT_NAME = "http-credentials-provider";

    @Override
    protected void setup(Binder binder)
    {
        install(credentialsProviderModule(
                HTTP_CREDENTIALS_PROVIDER_IDENTIFIER,
                HttpCredentialsProvider.class,
                innerBinder -> {
                    configBinder(innerBinder).bindConfig(HttpCredentialsProviderConfig.class);
                    innerBinder.bind(HttpCredentialsProvider.class);
                    httpClientBinder(innerBinder).bindHttpClient(HTTP_CREDENTIALS_PROVIDER_HTTP_CLIENT_NAME, ForHttpCredentialsProvider.class);
                    jsonCodecBinder(innerBinder).bindJsonCodec(Credentials.class);
                }));
    }
}
