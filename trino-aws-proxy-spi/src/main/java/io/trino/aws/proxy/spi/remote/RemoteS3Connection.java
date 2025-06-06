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
package io.trino.aws.proxy.spi.remote;

import io.trino.aws.proxy.spi.credentials.Credential;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface RemoteS3Connection
{
    Credential remoteCredential();

    default Optional<RemoteSessionRole> remoteSessionRole()
    {
        return Optional.empty();
    }

    default Optional<RemoteS3Facade> remoteS3Facade()
    {
        return Optional.empty();
    }

    record StaticRemoteS3Connection(
            Credential remoteCredential,
            Optional<RemoteSessionRole> remoteSessionRole,
            Optional<RemoteS3Facade> remoteS3Facade)
            implements RemoteS3Connection
    {
        public StaticRemoteS3Connection
        {
            requireNonNull(remoteCredential, "remoteCredential is null");
            requireNonNull(remoteSessionRole, "remoteSessionRole is null");
            requireNonNull(remoteS3Facade, "remoteS3Facade is null");
        }

        public StaticRemoteS3Connection(Credential remoteCredential)
        {
            this(remoteCredential, Optional.empty(), Optional.empty());
        }

        public StaticRemoteS3Connection(Credential remoteCredential, RemoteSessionRole remoteSessionRole)
        {
            this(remoteCredential, Optional.of(remoteSessionRole), Optional.empty());
        }
    }
}
