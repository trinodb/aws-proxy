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
package io.trino.s3.proxy.server.signing;

import io.trino.s3.proxy.server.credentials.Credentials;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SigningMetadata(SigningServiceType signingServiceType, Credentials credentials, Optional<String> session, String region, Optional<SigningContext> signingContext)
{
    public SigningMetadata
    {
        requireNonNull(signingServiceType, "signingService is null");
        requireNonNull(credentials, "credentials is null");
        requireNonNull(session, "session is null");
        requireNonNull(region, "region is null");
        requireNonNull(signingContext, "signingContext is null");
    }

    public SigningMetadata(SigningServiceType signingServiceType, Credentials credentials, Optional<String> session, String region)
    {
        this(signingServiceType, credentials, session, region, Optional.empty());
    }

    public SigningMetadata withSigningContext(SigningContext signingContext)
    {
        return new SigningMetadata(signingServiceType, credentials, session, region, Optional.of(signingContext));
    }
}
