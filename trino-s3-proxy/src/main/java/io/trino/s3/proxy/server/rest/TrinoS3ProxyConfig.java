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
package io.trino.s3.proxy.server.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class TrinoS3ProxyConfig
{
    private Optional<String> hostName = Optional.empty();

    @Config("s3proxy.hostname")
    @ConfigDescription("Hostname to use for REST operations, virtual-host style addressing is only supported if this is set")
    public TrinoS3ProxyConfig setHostName(String hostName)
    {
        this.hostName = Optional.ofNullable(hostName);
        return this;
    }

    @NotNull
    public Optional<String> getHostName()
    {
        return hostName;
    }
}
