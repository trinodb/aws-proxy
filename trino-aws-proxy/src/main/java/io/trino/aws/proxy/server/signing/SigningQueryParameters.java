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
package io.trino.aws.proxy.server.signing;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.collections.ImmutableMultiMap;
import io.trino.aws.proxy.spi.collections.MultiMap;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.timestamps.AwsTimestamp;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public record SigningQueryParameters(MultiMap passthroughQueryParameters, MultiMap signingQueryParameters)
{
    private static final Logger log = Logger.get(SigningQueryParameters.class);

    private static final Set<String> SIGNING_QUERY_PARAMETERS = ImmutableSet.of(
            "X-Amz-Date",
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-SignedHeaders",
            "X-Amz-Signature",
            "X-Amz-Expires");

    public SigningQueryParameters {
        passthroughQueryParameters = ImmutableMultiMap.copyOf(passthroughQueryParameters);
        signingQueryParameters = ImmutableMultiMap.copyOf(signingQueryParameters);
    }

    public static SigningQueryParameters splitQueryParameters(MultiMap allQueryParameters)
    {
        ImmutableMultiMap.Builder forwardableParamsBuilder = ImmutableMultiMap.builder(true);
        ImmutableMultiMap.Builder signingParamsBuilder = ImmutableMultiMap.builder(true);
        allQueryParameters.forEach((key, values) -> {
            if (SIGNING_QUERY_PARAMETERS.contains(key)) {
                signingParamsBuilder.addAll(key, values);
            }
            else {
                forwardableParamsBuilder.addAll(key, values);
            }
        });
        return new SigningQueryParameters(forwardableParamsBuilder.build(), signingParamsBuilder.build());
    }

    public Optional<String> signingAlgorithm()
    {
        return signingQueryParameters.getFirst("X-Amz-Algorithm");
    }

    public Optional<String> credential()
    {
        return signingQueryParameters.getFirst("X-Amz-Credential");
    }

    public Optional<String> signedHeaders()
    {
        return signingQueryParameters.getFirst("X-Amz-SignedHeaders");
    }

    public Optional<String> signature()
    {
        return signingQueryParameters.getFirst("X-Amz-Signature");
    }

    public Optional<Long> signatureExpiry()
    {
        return extractQueryParameter("X-Amz-Expires", Long::parseLong);
    }

    public Optional<Instant> requestDate()
    {
        return extractQueryParameter("X-Amz-Date", AwsTimestamp::fromRequestTimestamp);
    }

    public Optional<String> securityToken()
    {
        return signingQueryParameters.getFirst("X-Amz-Security-Token");
    }

    public Optional<RequestAuthorization> toRequestAuthorization()
    {
        try {
            return Optional.of(RequestAuthorization.presignedParse(
                    signingAlgorithm().orElseThrow(),
                    credential().orElseThrow(),
                    signedHeaders().orElseThrow(),
                    signature().orElseThrow(),
                    signatureExpiry().orElseThrow(),
                    requestDate().orElseThrow(),
                    securityToken()));
        }
        catch (NoSuchElementException _) {
        }

        return Optional.empty();
    }

    private <T> Optional<T> extractQueryParameter(String parameterName, Function<String, T> parser)
    {
        Optional<String> parameterValue = signingQueryParameters.getFirst(parameterName);
        try {
            return parameterValue.map(parser);
        }
        catch (Exception _) {
            log.debug("Invalid \"%s\" value in query parameters: %s", parameterName, parameterValue.orElse("<missing>"));
        }
        return Optional.empty();
    }
}
