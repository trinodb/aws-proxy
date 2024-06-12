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
import io.trino.s3.proxy.spi.rest.ParsedS3Request;
import io.trino.s3.proxy.spi.rest.Request;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriBuilder;
import org.glassfish.jersey.server.ContainerRequest;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;

class RequestBuilder
{
    private RequestBuilder() {}

    static Request fromRequest(ContainerRequest request)
    {
        Optional<Supplier<InputStream>> entitySupplier = request.hasEntity() ? Optional.of(request::getEntityStream) : Optional.empty();
        return new Request(request.getRequestUri(), request.getRequestHeaders(), request.getUriInfo().getQueryParameters(), request.getMethod(), entitySupplier);
    }

    static ParsedS3Request fromRequest(String requestPath, ContainerRequest request, Optional<String> serverHostName)
    {
        Optional<Supplier<InputStream>> entitySupplier = request.hasEntity() ? Optional.of(request::getEntityStream) : Optional.empty();
        String httpVerb = request.getMethod();
        MultivaluedMap<String, String> queryParameters = request.getUriInfo().getQueryParameters();
        MultivaluedMap<String, String> headers = lowercase(request.getHeaders());
        Optional<String> rawQuery = Optional.ofNullable(request.getRequestUri().getRawQuery());

        return serverHostName
                .flatMap(serverHostNameValue -> {
                    String lowercaseServerHostName = serverHostNameValue.toLowerCase(Locale.ROOT);
                    return Optional.ofNullable(headers.getFirst("host"))
                            .map(value -> UriBuilder.fromUri("http://" + value.toLowerCase(Locale.ROOT)).build().getHost())
                            .filter(value -> value.endsWith(lowercaseServerHostName))
                            .map(value -> value.substring(0, value.length() - lowercaseServerHostName.length()))
                            .map(value -> value.endsWith(".") ? value.substring(0, value.length() - 1) : value);
                })
                .map(bucket -> new ParsedS3Request(bucket, requestPath, headers, queryParameters, httpVerb, rawQuery, entitySupplier))
                .orElseGet(() -> {
                    List<String> parts = Splitter.on("/").limit(2).splitToList(requestPath);
                    if (parts.size() <= 1) {
                        return new ParsedS3Request(requestPath, "", headers, queryParameters, httpVerb, rawQuery, entitySupplier);
                    }
                    return new ParsedS3Request(parts.get(0), parts.get(1), headers, queryParameters, httpVerb, rawQuery, entitySupplier);
                });
    }
}
