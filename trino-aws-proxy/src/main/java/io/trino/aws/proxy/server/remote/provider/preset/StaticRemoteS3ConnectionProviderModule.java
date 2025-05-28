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
package io.trino.aws.proxy.server.remote.provider.preset;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.plugin.config.RemoteS3ConnectionProviderConfig;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection.StaticRemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;

import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class StaticRemoteS3ConnectionProviderModule
        extends AbstractConfigurationAwareModule
{
    public static final String STATIC_REMOTE_S3_CONNECTION_PROVIDER = "static";

    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                RemoteS3ConnectionProviderConfig.class,
                config -> config.getPluginIdentifier().map(STATIC_REMOTE_S3_CONNECTION_PROVIDER::equals).orElse(false),
                innerBinder -> {
                    StaticRemoteS3ConnectionProviderConfig config = buildConfigObject(StaticRemoteS3ConnectionProviderConfig.class);
                    newOptionalBinder(innerBinder, RemoteS3ConnectionProvider.class)
                            .setBinding()
                            .toInstance((_, _, _) -> Optional.of(new StaticRemoteS3Connection(new Credential(config.getAccessKey(), config.getSecretKey()))));
                }));
    }
}
