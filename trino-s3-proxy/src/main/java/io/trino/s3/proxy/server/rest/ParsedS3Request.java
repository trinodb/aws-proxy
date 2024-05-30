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
package io.trino.s3.proxy.server.rest;

import com.google.common.base.Splitter;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriBuilder;
import org.glassfish.jersey.server.ContainerRequest;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;
import static java.util.Objects.requireNonNull;

public record ParsedS3Request(
        String bucketName,
        String keyInBucket,
        MultivaluedMap<String, String> requestHeaders,
        MultivaluedMap<String, String> requestQueryParameters,
        String httpVerb,
        Optional<Supplier<InputStream>> entitySupplier)
{
    public ParsedS3Request
    {
        requireNonNull(bucketName, "bucketName is null");
        requireNonNull(keyInBucket, "keyInBucket is null");
        requestHeaders = lowercase(requireNonNull(requestHeaders, "requestHeaders is null"));
        requireNonNull(requestQueryParameters, "requestQueryParameters is null");
        requireNonNull(httpVerb, "httpVerb is null");
        requireNonNull(entitySupplier, "entitySupplier is null");
    }

    public static ParsedS3Request fromRequest(
            String requestPath,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> requestQueryParameters,
            String httpVerb,
            Optional<String> serverHostName,
            Optional<Supplier<InputStream>> entitySupplier)
    {
        MultivaluedMap<String, String> headers = lowercase(requestHeaders);
        return serverHostName
                .flatMap(serverHostNameValue -> {
                    String lowercaseServerHostName = serverHostNameValue.toLowerCase(Locale.ROOT);
                    return Optional.ofNullable(headers.getFirst("host"))
                            .map(value -> UriBuilder.fromUri("http://" + value.toLowerCase(Locale.ROOT)).build().getHost())
                            .filter(value -> value.endsWith(lowercaseServerHostName))
                            .map(value -> value.substring(0, value.length() - lowercaseServerHostName.length()))
                            .map(value -> value.endsWith(".") ? value.substring(0, value.length() - 1) : value);
                })
                .map(bucket -> new ParsedS3Request(bucket, requestPath, headers, requestQueryParameters, httpVerb, entitySupplier))
                .orElseGet(() -> {
                    List<String> parts = Splitter.on("/").limit(2).splitToList(requestPath);
                    if (parts.size() <= 1) {
                        return new ParsedS3Request(requestPath, "", headers, requestQueryParameters, httpVerb, entitySupplier);
                    }
                    return new ParsedS3Request(parts.get(0), parts.get(1), headers, requestQueryParameters, httpVerb, entitySupplier);
                });
    }

    public static ParsedS3Request fromRequest(String requestPath, ContainerRequest request, Optional<String> serverHostName)
    {
        Optional<Supplier<InputStream>> entitySupplier = request.hasEntity() ? Optional.of(request::getEntityStream) : Optional.empty();
        return fromRequest(requestPath, request.getHeaders(), request.getUriInfo().getQueryParameters(), request.getMethod(), serverHostName, entitySupplier);
    }
}
