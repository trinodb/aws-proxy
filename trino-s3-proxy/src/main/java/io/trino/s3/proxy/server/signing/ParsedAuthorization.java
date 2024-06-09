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
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

record ParsedAuthorization(String authorization, Credential credential, Set<String> signedHeaders, String signature)
{
    ParsedAuthorization
    {
        signedHeaders = ImmutableSet.copyOf(signedHeaders);
        requireNonNull(signature, "signature is null");
    }

    record Credential(String accessKey, String region)
    {
        Credential
        {
            requireNonNull(accessKey, "accessKey is null");
            requireNonNull(region, "region is null");
        }
    }

    boolean isValid()
    {
        return !signedHeaders.isEmpty() && !signature.isEmpty() && !credential.accessKey().isEmpty() && !credential.region().isEmpty();
    }

    static ParsedAuthorization parse(@Nullable String authorization)
    {
        if (authorization == null) {
            return new ParsedAuthorization("", new Credential("", ""), ImmutableSet.of(), "");
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
        Credential credential = (credentialList.size() > 2) ? new Credential(credentialList.getFirst(), credentialList.get(2)) : new Credential("", "");

        String signedHeadersSpec = parts.getOrDefault("SignedHeaders", "");
        Set<String> signedHeaders = Splitter.on(';').omitEmptyStrings().trimResults().splitToStream(signedHeadersSpec).collect(toImmutableSet());

        String signature = parts.getOrDefault("Signature", "");

        return new ParsedAuthorization(authorization, credential, signedHeaders, signature);
    }
}
