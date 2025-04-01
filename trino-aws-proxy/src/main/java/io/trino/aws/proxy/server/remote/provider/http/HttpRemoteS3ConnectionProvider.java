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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.server.remote.provider.SerializableRemoteS3Connection;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.net.URLEncoder;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HttpRemoteS3ConnectionProvider
        implements RemoteS3ConnectionProvider
{
    private final HttpClient httpClient;
    private final URI endpoint;
    private final EnumSet<RequestQuery> requestQueryFields;
    private final JsonCodec<SerializableRemoteS3Connection> responseCodec;
    private final ObjectMapper objectMapper;
    private final Optional<LoadingCache<Set<Entry<String, String>>, Optional<SerializableRemoteS3Connection>>> cache;

    @Inject
    public HttpRemoteS3ConnectionProvider(
            @ForHttpRemoteS3ConnectionProvider HttpClient httpClient,
            HttpRemoteS3ConnectionProviderConfig config,
            JsonCodec<SerializableRemoteS3Connection> responseCodec,
            ObjectMapper objectMapper)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.responseCodec = requireNonNull(responseCodec, "responseCodec is null");
        this.endpoint = config.getEndpoint();
        this.requestQueryFields = config.getRequestFields();
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        if (config.getCacheSize() > 0) {
            this.cache = Optional.of(newBuilder()
                    .maximumSize(config.getCacheSize())
                    .expireAfterWrite(config.getCacheTtl().toJavaTime())
                    .build(this::requestRemoteConnection));
        }
        else {
            this.cache = Optional.empty();
        }
    }

    @Override
    public Optional<? extends RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
    {
        Set<Map.Entry<String, String>> requestQueries = buildRequestQueries(signingMetadata, identity, request);
        return cache.map(actualCache -> actualCache.get(requestQueries))
                .orElseGet(() -> requestRemoteConnection(requestQueries));
    }

    @VisibleForTesting
    void resetCache()
    {
        cache.ifPresent(instantiatedCache -> {
            instantiatedCache.invalidateAll();
            instantiatedCache.cleanUp();
        });
    }

    private Set<Map.Entry<String, String>> buildRequestQueries(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
    {
        return requestQueryFields.stream()
                .map(requestField -> Map.entry(requestField.toString(), requestField.getValue(signingMetadata, identity, request, objectMapper)))
                .collect(toImmutableSet());
    }

    private Optional<SerializableRemoteS3Connection> requestRemoteConnection(Set<Map.Entry<String, String>> requestQueryParams)
    {
        UriBuilder uriBuilder = UriBuilder.fromUri(endpoint);
        requestQueryParams.forEach(param -> uriBuilder.queryParam(param.getKey().toLowerCase(Locale.ROOT), URLEncoder.encode(param.getValue(), UTF_8)));
        JsonResponse<SerializableRemoteS3Connection> response = httpClient.execute(
                prepareGet().setUri(uriBuilder.build()).build(),
                createFullJsonResponseHandler(responseCodec));
        HttpStatus statusCode = HttpStatus.fromStatusCode(response.getStatusCode());
        if (statusCode.family() != HttpStatus.Family.SUCCESSFUL) {
            if (statusCode == HttpStatus.NOT_FOUND) {
                return Optional.empty();
            }
            throw new RuntimeException("Failed to get remote S3 connection with HTTP plugin. Response code: " + statusCode + "; body: \n" + response.getResponseBody());
        }
        if (!response.hasValue()) {
            throw new RuntimeException("Failed to get remote S3 connection with HTTP plugin. Response code: " + statusCode + "; no body", response.getException());
        }
        return Optional.of(response.getValue());
    }
}
