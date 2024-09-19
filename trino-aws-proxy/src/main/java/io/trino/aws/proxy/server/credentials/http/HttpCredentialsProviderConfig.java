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
package io.trino.aws.proxy.server.credentials.http;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class HttpCredentialsProviderConfig
{
    private URI endpoint;
    private Map<String, String> httpHeaders = ImmutableMap.of();
    private long cacheSize;
    private Duration cacheTtl = Duration.ZERO;

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("credentials-provider.http.endpoint")
    @ConfigDescription("URL to retrieve credentials from, the username will be passed as a path under this URL")
    public HttpCredentialsProviderConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public Map<String, String> getHttpHeaders()
    {
        return httpHeaders;
    }

    @Config("credentials-provider.http.headers")
    @ConfigDescription(
            "Additional headers to include in requests, in the format header-1-name:header-1-value,header-2-name:header-2-value. " +
                    "If a header value needs to include a comma, it should be doubled")
    public HttpCredentialsProviderConfig setHttpHeaders(String httpHeadersList)
    {
        try {
            this.httpHeaders = Splitter.on(",").trimResults().omitEmptyStrings()
                    .splitToStream(httpHeadersList.replaceAll(",,", "\r"))
                    .map(item -> item.replace("\r", ","))
                    .map(s -> s.split(":", 2))
                    .collect(toImmutableMap(
                            a -> a[0].trim(),
                            a -> a[1].trim()));
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid HTTP header list: " + httpHeadersList);
        }
        return this;
    }

    @Config("credentials-provider.http.cache-size")
    @ConfigDescription("In-memory cache size for the credentials provider, defaults to 0 (no caching)")
    public HttpCredentialsProviderConfig setCacheSize(long cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    @Min(0)
    public long getCacheSize()
    {
        return cacheSize;
    }

    @Config("credentials-provider.http.cache-ttl")
    @ConfigDescription("In-memory cache TTL for the credentials provider, defaults to 0 seconds (no caching)")
    public HttpCredentialsProviderConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }
}
