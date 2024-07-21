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
package io.trino.aws.proxy.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TrinoAwsProxyConfig
{
    private Optional<String> s3HostName = Optional.empty();
    private String s3Path = "/api/v1/s3Proxy/s3";
    private String stsPath = "/api/v1/s3Proxy/sts";
    private Duration presignedUrlsDuration = new Duration(15, TimeUnit.MINUTES);
    private boolean generatePresignedUrlsOnHead = true;
    private String logsPath = "/api/v1/s3Proxy/logs";
    private int requestLoggerSavedQty = 10000;
    private Optional<DataSize> maxPayloadSize = Optional.empty();

    @Config("aws.proxy.s3.hostname")
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

    @Config("aws.proxy.s3.path")
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

    @Config("aws.proxy.sts.path")
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

    @MinDuration("1ms")
    public Duration getPresignedUrlsDuration()
    {
        return presignedUrlsDuration;
    }

    @Config("aws.proxy.s3.presigned-url.duration")
    @ConfigDescription("Duration of read/write pre-signed URLs")
    public TrinoAwsProxyConfig setPresignedUrlsDuration(Duration presignedUrlsDuration)
    {
        this.presignedUrlsDuration = requireNonNull(presignedUrlsDuration, "presignedUrlsDuration is null");
        return this;
    }

    public boolean isGeneratePresignedUrlsOnHead()
    {
        return generatePresignedUrlsOnHead;
    }

    @Config("aws.proxy.s3.presigned-url.head-generation.enabled")
    @ConfigDescription("Whether or not to generate pre-signed URL response headers on HEAD requests")
    public TrinoAwsProxyConfig setGeneratePresignedUrlsOnHead(boolean generatePresignedUrlsOnHead)
    {
        this.generatePresignedUrlsOnHead = generatePresignedUrlsOnHead;
        return this;
    }

    @Config("aws.proxy.logs.path")
    @ConfigDescription("URL Path for logs operations, optional")
    public TrinoAwsProxyConfig setLogsPath(String logsPath)
    {
        this.logsPath = logsPath;
        return this;
    }

    @NotNull
    public String getLogsPath()
    {
        return logsPath;
    }

    @Min(0)
    public int getRequestLoggerSavedQty()
    {
        return requestLoggerSavedQty;
    }

    @Config("aws.proxy.request.logger.saved-qty")
    @ConfigDescription("Number of log entries to store")
    public TrinoAwsProxyConfig setRequestLoggerSavedQty(int requestLoggerSavedQty)
    {
        this.requestLoggerSavedQty = requestLoggerSavedQty;
        return this;
    }

    @NotNull
    public Optional<DataSize> getMaxPayloadSize()
    {
        return maxPayloadSize;
    }

    @Config("aws.proxy.request.payload.max-size")
    @ConfigDescription("Max request/response payload size, optional")
    public TrinoAwsProxyConfig setMaxPayloadSize(DataSize maxPayloadSize)
    {
        this.maxPayloadSize = Optional.of(requireNonNull(maxPayloadSize, "requestByteQuota is null"));
        return this;
    }
}
