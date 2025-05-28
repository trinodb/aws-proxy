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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;

public class HttpRemoteS3ConnectionProviderConfig
{
    private URI endpoint;
    private EnumSet<RequestQuery> requestFields = EnumSet.allOf(RequestQuery.class);
    private long cacheSize;
    private Duration cacheTtl = Duration.valueOf("1s");

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("remote-s3-connection-provider.http.endpoint")
    public HttpRemoteS3ConnectionProviderConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    @NotEmpty
    public EnumSet<RequestQuery> getRequestFields()
    {
        return requestFields;
    }

    @Config("remote-s3-connection-provider.http.request-fields")
    public HttpRemoteS3ConnectionProviderConfig setRequestFields(List<RequestQuery> requestFields)
    {
        if (requestFields.isEmpty()) {
            this.requestFields = EnumSet.noneOf(RequestQuery.class);
        }
        else {
            this.requestFields = EnumSet.copyOf(requestFields);
        }
        return this;
    }

    @Min(0)
    public long getCacheSize()
    {
        return cacheSize;
    }

    @Config("remote-s3-connection-provider.http.cache-size")
    public HttpRemoteS3ConnectionProviderConfig setCacheSize(long cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    @NotNull
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("remote-s3-connection-provider.http.cache-ttl")
    public HttpRemoteS3ConnectionProviderConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }
}
