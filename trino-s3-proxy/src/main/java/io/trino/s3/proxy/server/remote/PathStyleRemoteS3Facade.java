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
package io.trino.s3.proxy.server.remote;

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PathStyleRemoteS3Facade
        implements RemoteS3Facade
{
    private final String domain;
    private final boolean https;
    private final Optional<Integer> port;

    public PathStyleRemoteS3Facade()
    {
        this("amazonaws.com", true, Optional.empty());
    }

    public PathStyleRemoteS3Facade(String domain, boolean https, Optional<Integer> port)
    {
        this.domain = requireNonNull(domain, "domain is null");
        this.port = requireNonNull(port, "port is null");
        this.https = https;
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        String host = buildHost(region);

        UriBuilder builder = uriBuilder.host(host)
                .scheme(https ? "https" : "http")
                .replacePath(bucket)
                .path(path);
        port.ifPresent(builder::port);

        return builder.build();
    }

    @VisibleForTesting
    protected String buildHost(String region)
    {
        return "s3.%s.%s".formatted(region, domain);
    }
}
