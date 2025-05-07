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
import io.trino.aws.proxy.server.remote.RemoteS3FacadeManager;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteS3FacadeFactory;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TestingRemoteS3FacadeManager
        extends RemoteS3FacadeManager
{
    private final AtomicReference<RemoteS3Facade> remoteS3Facade = new AtomicReference<>();

    @Inject
    public TestingRemoteS3FacadeManager(Set<RemoteS3FacadeFactory> factories)
    {
        super(factories);
    }

    @Override
    public void setDefaultRemoteS3Facade(RemoteS3Facade remoteS3Facade)
    {
        this.remoteS3Facade.set(remoteS3Facade);
    }

    @Override
    public URI remoteUri(String region)
    {
        return remoteS3Facade.get().remoteUri(region);
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return remoteS3Facade.get().buildEndpoint(uriBuilder, path, bucket, region);
    }

    @Override
    public void loadDefaultRemoteS3Facade()
    {
    }
}
