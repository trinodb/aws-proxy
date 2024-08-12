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
import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class HttpCredentialsProviderConfig
{
    private URI endpoint;
    private Map<String, String> httpHeaders = Map.of();

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("credentials-provider.http.endpoint")
    public HttpCredentialsProviderConfig setEndpoint(String endpoint)
    {
        this.endpoint = URI.create(endpoint);
        return this;
    }

    public Map<String, String> getHttpHeaders()
    {
        return httpHeaders;
    }

    @Config("credentials-provider.http.headers")
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
}
