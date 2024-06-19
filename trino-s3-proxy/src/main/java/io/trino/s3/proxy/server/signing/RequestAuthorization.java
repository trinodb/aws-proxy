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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public record RequestAuthorization(String authorization, String accessKey, String region, String keyPath, Set<String> lowercaseSignedHeaders, String signature, Optional<String> securityToken)
{
    public RequestAuthorization
    {
        requireNonNull(authorization, "authorization is null");
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(region, "region is null");
        requireNonNull(keyPath, "keyPath is null");
        lowercaseSignedHeaders = ImmutableSet.copyOf(lowercaseSignedHeaders);
        requireNonNull(signature, "signature is null");
        requireNonNull(securityToken, "securityToken is null");
    }

    boolean isValid()
    {
        return !lowercaseSignedHeaders.isEmpty() && !signature.isEmpty() && !accessKey.isEmpty() && !region.isEmpty();
    }

    public static RequestAuthorization parse(String authorization)
    {
        return parse(authorization, Optional.empty());
    }

    public static RequestAuthorization parse(String authorization, Optional<String> securityToken)
    {
        if (authorization.isBlank()) {
            return new RequestAuthorization("", "", "", "", ImmutableSet.of(), "", securityToken);
        }

        Map<String, String> parts;
        try {
            parts = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(authorization);
        }
        catch (IllegalArgumentException _) {
            parts = ImmutableMap.of();
        }

        String credentialSpec = parts.getOrDefault("AWS4-HMAC-SHA256 Credential", "");
        List<String> credentialList = Splitter.on('/').omitEmptyStrings().trimResults().splitToList(credentialSpec);

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

        String signedHeadersSpec = parts.getOrDefault("SignedHeaders", "");
        Set<String> signedHeaders = Splitter.on(';').omitEmptyStrings()
                .trimResults()
                .splitToStream(signedHeadersSpec)
                .map(header -> header.toLowerCase(Locale.ROOT))
                .collect(toImmutableSet());

        String signature = parts.getOrDefault("Signature", "");

        return new RequestAuthorization(authorization, accessKey, region, keyPath, signedHeaders, signature, securityToken);
    }
}
