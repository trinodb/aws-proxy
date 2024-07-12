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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public record RequestAuthorization(String accessKey, String region, String keyPath, Set<String> lowercaseSignedHeaders, String signature,
                                   Optional<Instant> expiry, Optional<String> securityToken)
{
    public static final RequestAuthorization INVALID = new RequestAuthorization("", "", "", ImmutableSet.of(), "", Optional.empty(), Optional.empty());
    private static final String SIGNATURE_ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String CREDENTIAL_HEADER = "%s Credential".formatted(SIGNATURE_ALGORITHM);

    public RequestAuthorization
    {
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(region, "region is null");
        requireNonNull(keyPath, "keyPath is null");
        lowercaseSignedHeaders = ImmutableSet.copyOf(lowercaseSignedHeaders);
        requireNonNull(signature, "signature is null");
        requireNonNull(expiry, "expiry is null");
        requireNonNull(securityToken, "securityToken is null");
    }

    public boolean isValid()
    {
        return !lowercaseSignedHeaders.isEmpty() && !signature.isEmpty() && !accessKey.isEmpty() && !region.isEmpty() && expiry.map(expiryTimestamp -> Instant.now().isBefore(expiryTimestamp)).orElse(true);
    }

    public String authorization()
    {
        checkState(expiry.isEmpty(), "authorization cannot be computed for an expiring request");

        return "%s=%s/%s, SignedHeaders=%s, Signature=%s".formatted(CREDENTIAL_HEADER, accessKey, keyPath, String.join(";", lowercaseSignedHeaders), signature);
    }

    public static RequestAuthorization parse(String authorization)
    {
        return parse(authorization, Optional.empty());
    }

    public static RequestAuthorization parse(String authorization, Optional<String> securityToken)
    {
        if (authorization.isBlank()) {
            return INVALID;
        }

        Map<String, String> parts;
        try {
            parts = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(authorization);
        }
        catch (IllegalArgumentException _) {
            parts = ImmutableMap.of();
        }

        String credentialSpec = parts.getOrDefault("%s Credential".formatted(SIGNATURE_ALGORITHM), "");
        return parse(credentialSpec, parts.getOrDefault("SignedHeaders", ""), parts.getOrDefault("Signature", ""), Optional.empty(), securityToken);
    }

    public static RequestAuthorization presignedParse(String algorithm, String credential, String signedHeaders, String signature, long expiry, Instant requestTimestamp, Optional<String> securityToken)
    {
        if (!SIGNATURE_ALGORITHM.equals(algorithm)) {
            return INVALID;
        }

        return parse(
                credential,
                signedHeaders,
                signature,
                Optional.of(requestTimestamp.plusSeconds(expiry)),
                securityToken);
    }

    private static RequestAuthorization parse(String credential, String signedHeaders, String signature, Optional<Instant> expiry, Optional<String> securityToken)
    {
        List<String> credentialList = Splitter.on('/').omitEmptyStrings().trimResults().splitToList(credential);

        String accessKey;
        String region;
        String keyPath;
        if (credentialList.size() > 2) {
            accessKey = credentialList.getFirst();
            region = credentialList.get(2);
            keyPath = String.join("/", credentialList.subList(1, credentialList.size()));
        }
        else {
            accessKey = "";
            region = "";
            keyPath = "";
        }

        Set<String> signedHeadersSet = Splitter.on(';').omitEmptyStrings()
                .trimResults()
                .splitToStream(signedHeaders)
                .map(header -> header.toLowerCase(Locale.ROOT))
                .collect(toImmutableSet());

        return new RequestAuthorization(accessKey, region, keyPath, signedHeadersSet, signature, expiry, securityToken);
    }
}
