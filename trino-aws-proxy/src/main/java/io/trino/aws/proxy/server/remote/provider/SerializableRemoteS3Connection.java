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
package io.trino.aws.proxy.server.remote.provider;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.aws.proxy.server.remote.DefaultRemoteS3Config;
import io.trino.aws.proxy.server.remote.PathStyleRemoteS3Facade;
import io.trino.aws.proxy.server.remote.VirtualHostStyleRemoteS3Facade;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record SerializableRemoteS3Connection(
        Credential remoteCredential,
        Optional<RemoteSessionRole> remoteSessionRole,
        Optional<RemoteS3Facade> remoteS3Facade)
        implements RemoteS3Connection
{
    public SerializableRemoteS3Connection
    {
        requireNonNull(remoteCredential, "remoteCredential is null");
        requireNonNull(remoteSessionRole, "remoteSessionRole is null");
        requireNonNull(remoteS3Facade, "remoteS3Facade is null");
    }

    @JsonCreator
    public static SerializableRemoteS3Connection fromConfig(
            @JsonProperty("remoteCredential") Credential remoteCredential,
            @JsonProperty("remoteSessionRole") Optional<RemoteSessionRole> remoteSessionRole,
            @JsonProperty("remoteS3FacadeConfiguration") Optional<Map<String, String>> remoteS3FacadeConfiguration)
    {
        Optional<RemoteS3Facade> facade = remoteS3FacadeConfiguration.map(config -> {
            ConfigurationFactory configurationFactory = new ConfigurationFactory(config);
            DefaultRemoteS3Config parsedConfig;
            try {
                parsedConfig = configurationFactory.build(DefaultRemoteS3Config.class);
            }
            catch (ConfigurationException e) {
                throw new IllegalArgumentException("Failed create RemoteS3Facade from RemoteS3FacadeConfiguration", e);
            }
            Set<String> unusedProperties = difference(configurationFactory.getProperties().keySet(), configurationFactory.getUsedProperties());
            if (!unusedProperties.isEmpty()) {
                throw new IllegalArgumentException(format("Failed to create RemoteS3Facade from RemoteS3FacadeConfiguration. Unused properties when instantiating " +
                        "DefaultRemoteS3Config: %s", unusedProperties));
            }
            return parsedConfig.getVirtualHostStyle() ? new VirtualHostStyleRemoteS3Facade(parsedConfig) : new PathStyleRemoteS3Facade(parsedConfig);
        });
        return new SerializableRemoteS3Connection(remoteCredential, remoteSessionRole, facade);
    }
}
