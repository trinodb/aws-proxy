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
package io.trino.aws.proxy.server.remote.provider.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class ConfigRemoteS3ConnectionProviderConfig
{
    private String accessKey;
    private String secretKey;

    @NotNull
    @NotEmpty
    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("remote-s3-connection-provider.access-key")
    public ConfigRemoteS3ConnectionProviderConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @NotNull
    @NotEmpty
    public String getSecretKey()
    {
        return secretKey;
    }

    @ConfigSecuritySensitive
    @Config("remote-s3-connection-provider.access-key")
    public ConfigRemoteS3ConnectionProviderConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }
}
