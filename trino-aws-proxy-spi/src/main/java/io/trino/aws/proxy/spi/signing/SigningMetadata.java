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
package io.trino.aws.proxy.spi.signing;

import io.trino.aws.proxy.spi.credentials.Credential;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// TODO: This can be in two states - with or without a signing context. The only time its ok for it not to have a signing context is when we pass it to `isValidAuthorization`.
//  We should refactor this class always have a SigningContext and pass something else to `SigningController`.
public record SigningMetadata(SigningServiceType signingServiceType, Credential credential, Optional<SigningContext> signingContext)
{
    public SigningMetadata
    {
        requireNonNull(signingServiceType, "signingService is null");
        requireNonNull(credential, "credential is null");
        requireNonNull(signingContext, "signingContext is null");
    }

    public SigningMetadata(SigningServiceType signingServiceType, Credential credential)
    {
        this(signingServiceType, credential, Optional.empty());
    }

    public SigningMetadata withSigningContext(SigningContext signingContext)
    {
        return new SigningMetadata(signingServiceType, credential, Optional.of(signingContext));
    }

    public SigningMetadata withCredential(Credential credential)
    {
        return new SigningMetadata(signingServiceType, credential, signingContext);
    }

    public SigningContext requiredSigningContext()
    {
        return signingContext().orElseThrow(() -> new IllegalArgumentException("Metadata does not contain a signing context"));
    }
}
