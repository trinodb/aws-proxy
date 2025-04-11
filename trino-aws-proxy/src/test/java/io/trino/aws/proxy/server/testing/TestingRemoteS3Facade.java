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
package io.trino.aws.proxy.server.testing;

import com.google.inject.Inject;
import io.trino.aws.proxy.server.remote.DefaultRemoteS3Config;
import io.trino.aws.proxy.server.remote.PathStyleRemoteS3Facade;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class TestingRemoteS3Facade
        implements RemoteS3Facade
{
    private final AtomicReference<RemoteS3Facade> delegate = new AtomicReference<>();

    @Inject
    public TestingRemoteS3Facade(S3Container s3Container)
    {
        delegate.set(new PathStyleRemoteS3Facade(new DefaultRemoteS3Config()
                .setDomain(s3Container.containerHost().getHost())
                .setPort(s3Container.containerHost().getPort())
                .setHttps(false)
                .setVirtualHostStyle(false)
                .setHostnameTemplate("${domain}")));
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return requireNonNull(delegate.get(), "delegate is null").buildEndpoint(uriBuilder, path, bucket, region);
    }

    public void setDelegate(RemoteS3Facade delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }
}
