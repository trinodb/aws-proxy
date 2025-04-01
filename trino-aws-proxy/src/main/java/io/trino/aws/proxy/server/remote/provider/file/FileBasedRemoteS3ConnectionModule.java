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
package io.trino.aws.proxy.server.remote.provider.file;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.remoteS3ConnectionProviderModule;

public class FileBasedRemoteS3ConnectionModule
        extends AbstractConfigurationAwareModule
{
    // set as config value for "remote-s3-connection-provider.type"
    public static final String FILE_BASED_REMOTE_S3_CONNECTION_PROVIDER = "file";

    @Override
    protected void setup(Binder binder)
    {
        install(remoteS3ConnectionProviderModule(
                FILE_BASED_REMOTE_S3_CONNECTION_PROVIDER,
                FileBasedRemoteS3ConnectionProvider.class,
                innerBinder -> {
                    configBinder(innerBinder).bindConfig(FileBasedRemoteS3ConnectionProviderConfig.class);
                    innerBinder.bind(FileBasedRemoteS3ConnectionProvider.class);
                    jsonCodecBinder(innerBinder).bindMapJsonCodec(String.class, RemoteS3Connection.class);
                }));
    }
}
