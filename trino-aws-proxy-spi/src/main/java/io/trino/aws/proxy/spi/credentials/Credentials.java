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
package io.trino.aws.proxy.spi.credentials;

import io.trino.aws.proxy.spi.remote.RemoteSessionRole;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record Credentials(Credential emulated, Optional<Credential> remote, Optional<RemoteSessionRole> remoteSessionRole, Optional<Identity> identity)
{
    public Credentials
    {
        requireNonNull(emulated, "emulated is null");
        requireNonNull(remote, "remote is null");
        requireNonNull(remoteSessionRole, "remoteSessionRole is null");
        requireNonNull(identity, "identity is null");
    }

    public Credential requiredRemoteCredential()
    {
        return remote.orElseThrow(() -> new IllegalStateException("Credentials are emulated only and cannot be used for remote access"));
    }

    public static Credentials build(Credential emulated)
    {
        return new Credentials(emulated, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static Credentials build(Credential emulated, Credential remote)
    {
        return new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.empty());
    }

    public static Credentials build(Credential emulated, Optional<Credential> remote)
    {
        return new Credentials(emulated, remote, Optional.empty(), Optional.empty());
    }

    public static Credentials build(Credential emulated, Credential remote, RemoteSessionRole remoteSessionRole)
    {
        return new Credentials(emulated, Optional.of(remote), Optional.of(remoteSessionRole), Optional.empty());
    }
}
