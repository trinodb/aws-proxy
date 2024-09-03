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
package io.trino.aws.proxy.server.security.opa;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.security.opa.OpaClient;
import io.trino.aws.proxy.spi.security.opa.OpaRequest;

import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.aws.proxy.spi.security.SecurityResponse.FAILURE;
import static io.trino.aws.proxy.spi.security.SecurityResponse.SUCCESS;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

public class DefaultOpaClient
        implements OpaClient
{
    private static final JsonCodec<Map<String, Object>> CODEC = mapJsonCodec(String.class, Object.class);

    private final HttpClient httpClient;

    @Inject
    public DefaultOpaClient(@ForOpa HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public SecurityResponse getSecurityResponse(OpaRequest request)
    {
        Map<String, Object> inputDocument = toInputDocument(request.document());

        Request.Builder builder = preparePost()
                .setUri(request.opaServerUri())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON_TYPE.getType())
                .setBodyGenerator(jsonBodyGenerator(CODEC, inputDocument));
        request.additionalHeaders().forEach((name, values) -> values.forEach(value -> builder.addHeader(name, value)));

        Map<String, Object> responseDocument = httpClient.execute(builder.build(), createJsonResponseHandler(CODEC));
        return toSecurityResponse(responseDocument);
    }

    protected Map<String, Object> toInputDocument(Map<String, Object> document)
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

    protected SecurityResponse toSecurityResponse(Map<String, Object> responseDocument)
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

    protected Optional<Boolean> extractBoolean(Map<?, ?> map, String key)
    {
        return switch (map.get(key)) {
            case Boolean b -> Optional.of(b);
            case null, default -> Optional.empty();
        };
    }
}
