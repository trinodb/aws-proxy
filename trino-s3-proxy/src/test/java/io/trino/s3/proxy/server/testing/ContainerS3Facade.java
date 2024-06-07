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
import io.trino.s3.proxy.server.remote.VirtualHostStyleRemoteS3Facade;

import java.util.Optional;

import static io.trino.s3.proxy.server.testing.TestingUtil.LOCALHOST_DOMAIN;

public final class ContainerS3Facade
{
    public static class PathStyleContainerS3Facade
            extends PathStyleRemoteS3Facade
    {
        @Inject
        public PathStyleContainerS3Facade(ManagedS3MockContainer s3MockContainer, TestingRemoteS3Facade delegatingFacade)
        {
            super((ignored1, ignored2) -> s3MockContainer.containerHost().getHost(), false, Optional.of(s3MockContainer.containerHost().getPort()));
            delegatingFacade.setDelegate(this);
        }
    }

    public static class VirtualHostStyleContainerS3Facade
            extends VirtualHostStyleRemoteS3Facade
    {
        @Inject
        public VirtualHostStyleContainerS3Facade(ManagedS3MockContainer s3MockContainer, TestingRemoteS3Facade delegatingFacade)
        {
            super((bucket, ignored) -> bucket.isEmpty() ? LOCALHOST_DOMAIN : "%s.%s".formatted(bucket, LOCALHOST_DOMAIN), false, Optional.of(s3MockContainer.containerHost().getPort()));
            delegatingFacade.setDelegate(this);
        }
    }
}
