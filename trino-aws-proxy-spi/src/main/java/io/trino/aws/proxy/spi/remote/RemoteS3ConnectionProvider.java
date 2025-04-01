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

import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.util.Optional;

// TODO: This should have a config implementation (hard-coding a single set of Remote Credentials) and an HTTP implementation
public interface RemoteS3ConnectionProvider
{
    RemoteS3ConnectionProvider NOOP = (_, _, _) -> Optional.empty();

    Optional<? extends RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request);
}
