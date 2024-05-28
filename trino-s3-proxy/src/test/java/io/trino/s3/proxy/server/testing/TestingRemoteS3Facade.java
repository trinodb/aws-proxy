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
package io.trino.s3.proxy.server.testing;

import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.remote.StandardS3RemoteS3Facade;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class TestingRemoteS3Facade
        implements RemoteS3Facade
{
    private volatile RemoteS3Facade delegate = new StandardS3RemoteS3Facade();

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return delegate.buildEndpoint(uriBuilder, path, bucket, region);
    }

    public void setDelegate(RemoteS3Facade delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }
}
