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
import io.trino.aws.proxy.server.remote.DefaultRemoteS3Module.ForDefaultRemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class DefaultRemoteS3Facade
        implements RemoteS3Facade
{
    private final RemoteS3Facade delegate;

    @Inject
    public DefaultRemoteS3Facade(@ForDefaultRemoteS3Facade RemoteS3Facade delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return delegate.buildEndpoint(uriBuilder, path, bucket, region);
    }
}
