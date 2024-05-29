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

import com.google.inject.Inject;
import io.trino.s3.proxy.server.remote.PathStyleRemoteS3Facade;

import java.util.Optional;

public class ContainerS3Facade
        extends PathStyleRemoteS3Facade
{
    private final ManagedS3MockContainer.ContainerHost containerHost;

    @Inject
    public ContainerS3Facade(ManagedS3MockContainer s3MockContainer, TestingRemoteS3Facade delegatingFacade)
    {
        this(s3MockContainer.getContainerHost(), delegatingFacade);
    }

    private ContainerS3Facade(ManagedS3MockContainer.ContainerHost containerHost, TestingRemoteS3Facade delegatingFacade)
    {
        super(containerHost.host(), false, Optional.of(containerHost.port()));
        this.containerHost = containerHost;
        delegatingFacade.setDelegate(this);
    }

    @Override
    protected String buildHost(String ignore)
    {
        return containerHost.host();
    }
}
