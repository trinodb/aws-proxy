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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.trino.aws.proxy.spi.signing.SigningTrait.S3V4_SIGNER;
import static io.trino.aws.proxy.spi.signing.SigningTrait.STREAM_CONTENT;
import static java.util.Objects.requireNonNull;

public record SigningServiceType(String serviceName, Set<SigningTrait> signingTraits)
{
    public static final SigningServiceType S3 = new SigningServiceType("s3", S3V4_SIGNER, STREAM_CONTENT);
    public static final SigningServiceType STS = new SigningServiceType("sts");
    public static final SigningServiceType LOGS = new SigningServiceType("logs");

    public SigningServiceType
    {
        requireNonNull(serviceName, "serviceName is null");
        signingTraits = ImmutableSet.copyOf(signingTraits);
    }

    public SigningServiceType(String serviceName, SigningTrait... signingTraits)
    {
        this(serviceName, ImmutableSet.copyOf(signingTraits));
    }

    public boolean hasTrait(SigningTrait signingTrait)
    {
        return signingTraits.contains(signingTrait);
    }
}
