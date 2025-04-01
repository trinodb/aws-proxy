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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public enum RequestQuery
{
    BUCKET((_, _, request, _) -> request.bucketName()),
    KEY((_, _, request, _) -> request.keyInBucket()),
    EMULATED_ACCESS_KEY((signingMetadata, _, _, _) -> signingMetadata.credential().accessKey()),
    IDENTITY((_, identity, _, objectMapper) -> identity.map(value -> {
        try {
            return objectMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException exception) {
            throw new RuntimeException(exception);
        }
    }).orElse(""));

    private final FieldSelector selector;

    RequestQuery(FieldSelector selector)
    {
        this.selector = requireNonNull(selector, "selector is null");
    }

    public String getValue(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request, ObjectMapper objectMapper)
    {
        return selector.apply(signingMetadata, identity, request, objectMapper);
    }

    @FunctionalInterface
    private interface FieldSelector
    {
        String apply(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request, ObjectMapper objectMapper);
    }
}
