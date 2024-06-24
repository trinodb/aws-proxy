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
package io.trino.aws.proxy.server.rest;

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.collections.ImmutableMultiMap;
import io.trino.aws.proxy.spi.collections.MultiMap;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestContent.ContentType;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import org.glassfish.jersey.server.ContainerRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static com.google.common.io.ByteStreams.toByteArray;

class RequestBuilder
{
    private static final Logger log = Logger.get(RequestBuilder.class);

    private RequestBuilder() {}

    static Request fromRequest(ContainerRequest request)
    {
        MultiMap requestHeaders = ImmutableMultiMap.copyOfCaseInsensitive(request.getHeaders().entrySet());
        String xAmzDate = requestHeaders.getFirst("x-amz-date").orElseThrow(() -> {
            log.debug("Missing \"x-amz-date\" header");
            return new WebApplicationException(Response.Status.BAD_REQUEST);
        });
        Optional<String> securityTokenHeader = requestHeaders.getFirst("x-amz-security-token");
        RequestContent requestContent = request.hasEntity() ? buildRequestContent(request.getEntityStream(), getRequestContentTypeFromHeader(requestHeaders)) : RequestContent.EMPTY;
        return new Request(
                UUID.randomUUID(),
                RequestAuthorization.parse(requestHeaders.getFirst("authorization").orElse(""), securityTokenHeader),
                xAmzDate,
                request.getRequestUri(),
                requestHeaders,
                ImmutableMultiMap.copyOf(request.getUriInfo().getQueryParameters().entrySet()),
                request.getMethod(),
                requestContent);
    }

    static ParsedS3Request fromRequest(Request request, String requestPath, Optional<String> serverHostName)
    {
        String httpVerb = request.httpVerb();
        MultiMap queryParameters = request.requestQueryParameters();
        MultiMap headers = request.requestHeaders();
        Optional<String> rawQuery = Optional.ofNullable(request.requestUri().getRawQuery());
        RequestContent requestContent = request.requestContent();

        record BucketAndKey(String bucket, String rawKey) {}

        BucketAndKey bucketAndKey = serverHostName
                .flatMap(serverHostNameValue -> {
                    String lowercaseServerHostName = serverHostNameValue.toLowerCase(Locale.ROOT);
                    return headers.getFirst("host")
                            .map(value -> UriBuilder.fromUri("http://" + value.toLowerCase(Locale.ROOT)).build().getHost())
                            .filter(value -> value.endsWith(lowercaseServerHostName))
                            .map(value -> value.substring(0, value.length() - lowercaseServerHostName.length()))
                            .map(value -> value.endsWith(".") ? value.substring(0, value.length() - 1) : value);
                })
                .map(bucket -> new BucketAndKey(bucket, requestPath))
                .orElseGet(() -> {
                    List<String> parts = Splitter.on("/").limit(2).splitToList(requestPath);
                    if (parts.size() <= 1) {
                        return new BucketAndKey(requestPath, "");
                    }
                    return new BucketAndKey(parts.get(0), parts.get(1));
                });

        String keyInBucket = URLDecoder.decode(bucketAndKey.rawKey, StandardCharsets.UTF_8);
        return new ParsedS3Request(
                request.requestId(),
                request.requestAuthorization(),
                request.requestDate(),
                bucketAndKey.bucket,
                keyInBucket,
                headers,
                queryParameters,
                httpVerb,
                bucketAndKey.rawKey,
                rawQuery,
                requestContent);
    }

    private static RequestContent buildRequestContent(InputStream requestEntityStream, String requestContentType)
    {
        ContentType contentType = switch (requestContentType) {
            case "aws-chunked" -> ContentType.AWS_CHUNKED;
            case "chunked" -> ContentType.W3C_CHUNKED;
            default -> ContentType.STANDARD;
        };

        Supplier<Optional<byte[]>> bytesSupplier;
        if (contentType == ContentType.STANDARD) {
            // memoize the entity bytes so it can be called multiple times
            bytesSupplier = Suppliers.memoize(() -> {
                try {
                    return Optional.of(toByteArray(requestEntityStream));
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
                        .or(() -> Optional.of(requestEntityStream));
            }
        };
    }

    private static String getRequestContentTypeFromHeader(MultiMap requestHeaders)
    {
        return requestHeaders.getFirst("content-encoding").or(() -> requestHeaders.getFirst("transfer-encoding")).orElse("");
    }
}
