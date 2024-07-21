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
package io.trino.aws.proxy.server.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class TrinoAwsProxyConfig
{
    private Optional<String> s3HostName = Optional.empty();
    private String s3Path = "/api/v1/s3Proxy/s3";
    private String stsPath = "/api/v1/s3Proxy/sts";

    @Config("s3proxy.s3.hostname")
    @ConfigDescription("Hostname to use for S3 REST operations, virtual-host style addressing is only supported if this is set")
    public TrinoAwsProxyConfig setS3HostName(String s3HostName)
    {
        this.s3HostName = Optional.ofNullable(s3HostName);
        return this;
    }

    @NotNull
    public Optional<String> getS3HostName()
    {
        return s3HostName;
    }

    @Config("s3proxy.s3.path")
    @ConfigDescription("URL Path for S3 operations, optional")
    public TrinoAwsProxyConfig setS3Path(String s3Path)
    {
        this.s3Path = s3Path;
        return this;
    }

    @NotNull
    public String getS3Path()
    {
        return s3Path;
    }

    @Config("s3proxy.sts.path")
    @ConfigDescription("URL Path for STS operations, optional")
    public TrinoAwsProxyConfig setStsPath(String stsPath)
    {
        this.stsPath = stsPath;
        return this;
    }

    @NotNull
    public String getStsPath()
    {
        return stsPath;
    }
}
