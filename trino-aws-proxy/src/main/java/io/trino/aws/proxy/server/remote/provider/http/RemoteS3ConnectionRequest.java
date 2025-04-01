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
package io.trino.aws.proxy.server.remote.provider.http;

import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record RemoteS3ConnectionRequest(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
{
    public RemoteS3ConnectionRequest
    {
        requireNonNull(signingMetadata, "signingMetadata is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(request, "request is null");
    }
}
