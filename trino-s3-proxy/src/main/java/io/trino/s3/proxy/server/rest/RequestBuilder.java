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
import com.google.common.base.Suppliers;
import io.trino.s3.proxy.server.rest.RequestContent.ContentType;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.commons.httpclient.ChunkedInputStream;
import org.glassfish.jersey.server.ContainerRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;

class RequestBuilder
{
    private RequestBuilder() {}

    static Request fromRequest(ContainerRequest request)
    {
        RequestContent requestContent = buildRequestContent(request);
        return new Request(request.getRequestUri(), request.getRequestHeaders(), request.getUriInfo().getQueryParameters(), request.getMethod(), requestContent);
    }

    static ParsedS3Request fromRequest(Request request, String requestPath, Optional<String> serverHostName)
    {
        String httpVerb = request.httpVerb();
        MultivaluedMap<String, String> queryParameters = request.requestQueryParameters();
        MultivaluedMap<String, String> headers = lowercase(request.requestHeaders());
        Optional<String> rawQuery = Optional.ofNullable(request.requestUri().getRawQuery());
        RequestContent requestContent = request.requestContent();

        return serverHostName
                .flatMap(serverHostNameValue -> {
                    String lowercaseServerHostName = serverHostNameValue.toLowerCase(Locale.ROOT);
                    return Optional.ofNullable(headers.getFirst("host"))
                            .map(value -> UriBuilder.fromUri("http://" + value.toLowerCase(Locale.ROOT)).build().getHost())
                            .filter(value -> value.endsWith(lowercaseServerHostName))
                            .map(value -> value.substring(0, value.length() - lowercaseServerHostName.length()))
                            .map(value -> value.endsWith(".") ? value.substring(0, value.length() - 1) : value);
                })
                .map(bucket -> new ParsedS3Request(bucket, requestPath, headers, queryParameters, httpVerb, rawQuery, requestContent))
                .orElseGet(() -> {
                    List<String> parts = Splitter.on("/").limit(2).splitToList(requestPath);
                    if (parts.size() <= 1) {
                        return new ParsedS3Request(requestPath, "", headers, queryParameters, httpVerb, rawQuery, requestContent);
                    }
                    return new ParsedS3Request(parts.get(0), parts.get(1), headers, queryParameters, httpVerb, rawQuery, requestContent);
                });
    }

    private static RequestContent buildRequestContent(ContainerRequest request)
    {
        if (!request.hasEntity()) {
            return RequestContent.EMPTY;
        }

        ContentType contentType = switch (encoding(request.getRequestHeaders())) {
            case "aws-chunked" -> ContentType.AWS_CHUNKED;
            case "chunked" -> ContentType.W3C_CHUNKED;
            default -> ContentType.STANDARD;
        };

        Supplier<InputStream> inputStreamSupplier = () -> buildInputStream(request.getEntityStream(), contentType);

        Supplier<Optional<byte[]>> bytesSupplier;
        if (contentType == ContentType.STANDARD) {
            // memoize the entity bytes so it can be called multiple times
            bytesSupplier = Suppliers.memoize(() -> {
                try {
                    return Optional.of(toByteArray(inputStreamSupplier.get()));
                }
                catch (IOException e) {
                    throw new WebApplicationException(Response.Status.BAD_REQUEST);
                }
            });
        }
        else {
            // we always stream chunked content. Never load it into memory.
            bytesSupplier = Optional::empty;
        }

        return new RequestContent()
        {
            @Override
            public Optional<Integer> contentLength()
            {
                return bytesSupplier.get().map(bytes -> bytes.length);
            }

            @Override
            public ContentType contentType()
            {
                return contentType;
            }

            @Override
            public Optional<byte[]> standardBytes()
            {
                return bytesSupplier.get();
            }

            @Override
            public Optional<InputStream> inputStream()
            {
                return standardBytes()
                        .map(bytes -> (InputStream) new ByteArrayInputStream(bytes))
                        .or(() -> Optional.of(inputStreamSupplier.get()));
            }
        };
    }

    private static InputStream buildInputStream(InputStream entityStream, ContentType contentType)
    {
        return (contentType == ContentType.AWS_CHUNKED) ? awsChunkedStream(entityStream) : entityStream;
    }

    private static InputStream awsChunkedStream(InputStream inputStream)
    {
        // TODO do we need to add a Jersey MessageBodyWriter that handles aws-chunked?

        // TODO move this into a Jersey MessageBodyReader
        try {
            // AWS's custom chunked encoding doesn't get handled by Jersey. Do it manually.
            return new ChunkedInputStream(inputStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String encoding(MultivaluedMap<String, String> requestHeaders)
    {
        return firstNonNull(requestHeaders.getFirst("content-encoding"), firstNonNull(requestHeaders.getFirst("transfer-encoding"), ""));
    }
}
