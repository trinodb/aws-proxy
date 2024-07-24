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
package io.trino.aws.proxy.server.remote;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class RemoteS3Config
{
    private boolean https = true;
    private String domain = "amazonaws.com";
    private Optional<Integer> port = Optional.empty();
    private boolean virtualHostStyle = true;
    private String hostnameTemplate = "${bucket}.s3.${region}.${domain}";

    @Config("remoteS3.https")
    public RemoteS3Config setHttps(boolean https)
    {
        this.https = https;
        return this;
    }

    @Config("remoteS3.domain")
    public RemoteS3Config setDomain(String s3Domain)
    {
        this.domain = s3Domain;
        return this;
    }

    @Config("remoteS3.port")
    public RemoteS3Config setPort(Integer port)
    {
        this.port = Optional.ofNullable(port);
        return this;
    }

    @Config("remoteS3.virtual-host-style")
    public RemoteS3Config setVirtualHostStyle(boolean virtualHostStyle)
    {
        this.virtualHostStyle = virtualHostStyle;
        return this;
    }

    @Config("remoteS3.hostname.template")
    public RemoteS3Config setHostnameTemplate(String hostnameTemplate)
    {
        this.hostnameTemplate = hostnameTemplate;
        return this;
    }

    public boolean getHttps()
    {
        return https;
    }

    @NotNull
    public String getDomain()
    {
        return domain;
    }

    @NotNull
    public Optional<@Min(1) @Max(65535) Integer> getPort()
    {
        return port;
    }

    public boolean getVirtualHostStyle()
    {
        return virtualHostStyle;
    }

    @NotNull
    public String getHostnameTemplate()
    {
        return hostnameTemplate;
    }
}
