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

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.S3SecurityFacade;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.security.opa.OpaRequest;
import io.trino.aws.proxy.spi.security.opa.OpaS3SecurityMapper;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

public class OpaS3SecurityFacadeProvider
        implements S3SecurityFacadeProvider
{
    private static final JsonCodec<Map<String, Object>> CODEC = mapJsonCodec(String.class, Object.class);

    private final HttpClient httpClient;
    private final URI opaServerUri;
    private final OpaS3SecurityMapper opaS3SecurityMapper;

    @Inject
    public OpaS3SecurityFacadeProvider(@ForOpa HttpClient httpClient, OpaS3SecurityMapper opaS3SecurityMapper, OpaS3SecurityConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.opaS3SecurityMapper = requireNonNull(opaS3SecurityMapper, "opaS3SecurityMapper is null");
        opaServerUri = UriBuilder.fromUri(config.getOpaServerBaseUri()).build();
    }

    @Override
    public S3SecurityFacade securityFacadeForRequest(ParsedS3Request request)
            throws WebApplicationException
    {
        return lowercaseAction -> facade(request, lowercaseAction);
    }

    private SecurityResponse facade(ParsedS3Request parsedS3Request, Optional<String> lowercaseAction)
    {
        OpaRequest opaRequest = opaS3SecurityMapper.toRequest(parsedS3Request, lowercaseAction, opaServerUri);

        Map<String, Object> inputDocument = opaS3SecurityMapper.toInputDocument(opaRequest.document());

        Request.Builder builder = preparePost()
                .setUri(opaRequest.opaServerUri())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON_TYPE.getType())
                .setBodyGenerator(jsonBodyGenerator(CODEC, inputDocument));
        opaRequest.additionalHeaders().forEach((name, values) -> values.forEach(value -> builder.addHeader(name, value)));

        Map<String, Object> responseDocument = httpClient.execute(builder.build(), createJsonResponseHandler(CODEC));
        return opaS3SecurityMapper.toSecurityResponse(responseDocument);
    }
}
