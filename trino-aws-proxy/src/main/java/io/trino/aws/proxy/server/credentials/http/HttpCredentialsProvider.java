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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

public class HttpCredentialsProvider
        implements CredentialsProvider
{
    private record CredentialsKey(String emulatedAccessKey, Optional<String> session)
    {
        private CredentialsKey {
            requireNonNull(emulatedAccessKey, "emulatedAccessKey is null");
            requireNonNull(session, "session is null");
        }
    }

    private final HttpClient httpClient;
    private final JsonCodec<Credentials> jsonCodec;
    private final URI httpCredentialsProviderEndpoint;
    private final Map<String, String> httpHeaders;
    private final Optional<LoadingCache<CredentialsKey, Optional<Credentials>>> credentialsCache;
    private final Function<CredentialsKey, Optional<Credentials>> credentialsFetcher;

    @Inject
    public HttpCredentialsProvider(@ForHttpCredentialsProvider HttpClient httpClient, HttpCredentialsProviderConfig config, JsonCodec<Credentials> jsonCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.httpCredentialsProviderEndpoint = config.getEndpoint();
        this.httpHeaders = ImmutableMap.copyOf(config.getHttpHeaders());
        if (config.getCacheSize() > 0 && config.getCacheTtl().toMillis() > 0) {
            LoadingCache<CredentialsKey, Optional<Credentials>> cache = Caffeine.newBuilder()
                    .maximumSize(config.getCacheSize())
                    .expireAfterWrite(config.getCacheTtl().toJavaTime())
                    .build(this::fetchCredentials);
            this.credentialsCache = Optional.of(cache);
            this.credentialsFetcher = cache::get;
        }
        else {
            this.credentialsCache = Optional.empty();
            this.credentialsFetcher = this::fetchCredentials;
        }
    }

    @Override
    public Optional<Credentials> credentials(String emulatedAccessKey, Optional<String> session)
    {
        return credentialsFetcher.apply(new CredentialsKey(emulatedAccessKey, session));
    }

    @VisibleForTesting
    void resetCache()
    {
        credentialsCache.ifPresent(instantiatedCache -> {
            instantiatedCache.invalidateAll();
            instantiatedCache.cleanUp();
        });
    }

    private Optional<Credentials> fetchCredentials(CredentialsKey credentialsKey)
    {
        UriBuilder uriBuilder = UriBuilder.fromUri(httpCredentialsProviderEndpoint).path(credentialsKey.emulatedAccessKey());
        credentialsKey.session().ifPresent(sessionToken -> uriBuilder.queryParam("sessionToken", sessionToken));
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
