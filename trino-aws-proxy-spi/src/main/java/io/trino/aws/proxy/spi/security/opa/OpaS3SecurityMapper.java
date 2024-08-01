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
package io.trino.aws.proxy.spi.security.opa;

import com.google.common.collect.ImmutableMap;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.SecurityResponse;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.trino.aws.proxy.spi.security.SecurityResponse.FAILURE;
import static io.trino.aws.proxy.spi.security.SecurityResponse.SUCCESS;

@SuppressWarnings("SameParameterValue")
@FunctionalInterface
public interface OpaS3SecurityMapper
{
    default Map<String, Object> toInputDocument(Map<String, Object> document)
    {
        /*
            Default formats standard input:

            {
                "input": {
                    ... nested document ...
                }
            }
        */

        return ImmutableMap.of("input", document);
    }

    default SecurityResponse toSecurityResponse(Map<String, Object> responseDocument)
    {
        /*
            Default handles standard response:

            {
                "result": true/false
            }
        */

        return extractBoolean(responseDocument, "result")
                .map(allowed -> allowed ? SUCCESS : FAILURE)
                .orElse(FAILURE);
    }

    OpaRequest toRequest(ParsedS3Request request, Optional<String> lowercaseAction, URI baseUri);

    static Optional<Boolean> extractBoolean(Map<?, ?> map, String key)
    {
        return switch (map.get(key)) {
            case Boolean b -> Optional.of(b);
            case null, default -> Optional.empty();
        };
    }
}
