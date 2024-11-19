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

import com.google.inject.Inject;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PathStyleRemoteS3Facade
        implements RemoteS3Facade
{
    private final RemoteS3HostBuilder hostBuilder;
    private final boolean https;
    private final Optional<Integer> port;

    @Inject
    public PathStyleRemoteS3Facade(DefaultRemoteS3Config remoteS3Config)
    {
        this((bucket, region) -> RemoteS3HostBuilder.getHostName(bucket, region, remoteS3Config.getDomain(), remoteS3Config.getHostnameTemplate()), remoteS3Config.getHttps(), remoteS3Config.getPort());
    }

    public PathStyleRemoteS3Facade(RemoteS3HostBuilder hostBuilder, boolean https, Optional<Integer> port)
    {
        this.hostBuilder = requireNonNull(hostBuilder, "hostBuilder is null");
        this.port = requireNonNull(port, "port is null");
        this.https = https;
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        String host = hostBuilder.build(bucket, region);

        UriBuilder builder = uriBuilder.host(host)
                .scheme(https ? "https" : "http")
                .replacePath(bucket)
                .path(path);
        port.ifPresent(builder::port);

        return builder.build();
    }
}
