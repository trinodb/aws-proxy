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
package io.trino.aws.proxy.server.credentials.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.Identity;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

public class HttpCredentialsProvider
        implements CredentialsProvider
{
    private final HttpClient httpClient;
    private final JsonCodec<Credentials> jsonCodec;
    private final URI httpCredentialsProviderEndpoint;
    private final Map<String, String> httpHeaders;

    @Inject
    public HttpCredentialsProvider(@ForHttpCredentialsProvider HttpClient httpClient, HttpCredentialsProviderConfig config, ObjectMapper objectMapper, Class<? extends Identity> identityClass)
    {
        requireNonNull(objectMapper, "objectMapper is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.httpCredentialsProviderEndpoint = config.getEndpoint();
        ObjectMapper adjustedObjectMapper = objectMapper.registerModule(new SimpleModule().addAbstractTypeMapping(Identity.class, identityClass));
        this.jsonCodec = new JsonCodecFactory(() -> adjustedObjectMapper).jsonCodec(Credentials.class);
        this.httpHeaders = ImmutableMap.copyOf(config.getHttpHeaders());
    }

    @Override
    public Optional<Credentials> credentials(String emulatedAccessKey, Optional<String> session)
    {
        UriBuilder uriBuilder = UriBuilder.fromUri(httpCredentialsProviderEndpoint).path(emulatedAccessKey);
        session.ifPresent(sessionToken -> uriBuilder.queryParam("sessionToken", sessionToken));
        Request.Builder requestBuilder = prepareGet()
                .addHeaders(Multimaps.forMap(httpHeaders))
                .setUri(uriBuilder.build());
        JsonResponse<Credentials> response = httpClient.execute(requestBuilder.build(), createFullJsonResponseHandler(jsonCodec));
        if (response.getStatusCode() == HttpStatus.NOT_FOUND.code() || !response.hasValue()) {
            return Optional.empty();
        }
        return Optional.of(response.getValue());
    }
}
