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
package io.trino.aws.proxy.server.remote.provider.http;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.server.remote.provider.SerializableRemoteS3Connection;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.remoteS3ConnectionProviderModule;

public class HttpRemoteS3ConnectionProviderModule
        extends AbstractConfigurationAwareModule
{
    public static final String HTTP_REMOTE_S3_CONNECTION_PROVIDER = "http";

    @Override
    protected void setup(Binder binder)
    {
        install(remoteS3ConnectionProviderModule(
                HTTP_REMOTE_S3_CONNECTION_PROVIDER,
                HttpRemoteS3ConnectionProvider.class,
                innerBinder -> {
                    httpClientBinder(innerBinder).bindHttpClient("remote-s3-connection-provider.http", ForHttpRemoteS3ConnectionProvider.class);
                    configBinder(innerBinder).bindConfig(HttpRemoteS3ConnectionProviderConfig.class);
                    innerBinder.bind(HttpRemoteS3ConnectionProvider.class);
                    jsonCodecBinder(innerBinder).bindJsonCodec(SerializableRemoteS3Connection.class);
                }));
    }
}
