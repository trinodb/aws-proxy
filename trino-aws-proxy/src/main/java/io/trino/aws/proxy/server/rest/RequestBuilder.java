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
import io.trino.aws.proxy.server.rest.RequestHeadersBuilder.InternalRequestHeaders;
import io.trino.aws.proxy.server.signing.SigningQueryParameters;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestContent.ContentType;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.UriBuilder;
import org.glassfish.jersey.server.ContainerRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static com.google.common.io.ByteStreams.toByteArray;
import static io.trino.aws.proxy.server.signing.SigningQueryParameters.splitQueryParameters;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;

class RequestBuilder
{
    private static final Logger log = Logger.get(RequestBuilder.class);

    private RequestBuilder() {}

    static Request fromRequest(ContainerRequest request)
    {
        InternalRequestHeaders requestHeaders = RequestHeadersBuilder.parseHeaders(ImmutableMultiMap.copyOfCaseInsensitive(request.getHeaders().entrySet()));
        Optional<Instant> requestTimestamp;

        RequestContent requestContent = request.hasEntity() ? buildRequestContent(request.getEntityStream(), requestHeaders) : RequestContent.EMPTY;
        SigningQueryParameters signingQueryParameters = splitQueryParameters(ImmutableMultiMap.copyOf(request.getUriInfo().getQueryParameters(true).entrySet()));

        Optional<RequestAuthorization> requestAuthorization = requestHeaders.requestAuthorization();
        if (requestAuthorization.isPresent()) {
            requestTimestamp = requestHeaders.requestDate();
        }
        else {
            requestAuthorization = signingQueryParameters.toRequestAuthorization();
            requestTimestamp = signingQueryParameters.requestDate();
        }
        return new Request(
                UUID.randomUUID(),
                requestAuthorization.orElseThrow(() -> {
                    log.debug("request authorization is missing");
                    return new WebApplicationException(BAD_REQUEST);
                }),
                requestTimestamp.orElseThrow(() -> {
                    log.debug("Missing request date");
                    return new WebApplicationException(BAD_REQUEST);
                }),
                request.getRequestUri(),
                requestHeaders.requestHeaders(),
                signingQueryParameters.passthroughQueryParameters(),
                request.getMethod(),
                requestContent);
    }

    static ParsedS3Request fromRequest(Request request, String requestPath, Optional<String> serverHostName)
    {
        String httpVerb = request.httpVerb();
        MultiMap queryParameters = request.requestQueryParameters();
        RequestHeaders headers = request.requestHeaders();
        Optional<String> rawQuery = Optional.ofNullable(request.requestUri().getRawQuery());
        RequestContent requestContent = request.requestContent();

        record BucketAndKey(String bucket, String rawKey) {}

        BucketAndKey bucketAndKey = serverHostName
                .flatMap(serverHostNameValue -> {
                    String lowercaseServerHostName = serverHostNameValue.toLowerCase(Locale.ROOT);
                    return headers.unmodifiedHeaders().getFirst("host")
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

        return new ParsedS3Request(
                request.requestId(),
                request.requestAuthorization(),
                request.requestDate(),
                bucketAndKey.bucket,
                decodeUriComponent(bucketAndKey.rawKey()),
                headers,
                queryParameters,
                httpVerb,
                bucketAndKey.rawKey,
                rawQuery,
                requestContent);
    }

    private static String decodeUriComponent(String component)
    {
        return URLDecoder.decode(component, StandardCharsets.UTF_8);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private static RequestContent buildRequestContent(InputStream requestEntityStream, InternalRequestHeaders requestHeaders)
    {
        ContentType contentType = requestHeaders.requestPayloadContentType().orElse(ContentType.STANDARD);

        Supplier<Optional<byte[]>> bytesSupplier = switch (contentType) {
            case STANDARD -> Suppliers.memoize(() -> {
                try {
                    return Optional.of(toByteArray(requestEntityStream));
                }
                catch (IOException e) {
                    throw new WebApplicationException(BAD_REQUEST);
                }
            });

            default -> Optional::empty;
        };

        Supplier<Optional<Integer>> contentLengthSupplier = switch (contentType) {
            case STANDARD -> () -> bytesSupplier.get().map(bytes -> bytes.length);

            // AWS does not mandate x-amz-decoded-content length is required for chunked transfer encoding
            // But we require it for simplicity (Content-Length is needed since we don't do chunking on outbound requests)
            case AWS_CHUNKED, W3C_CHUNKED, AWS_CHUNKED_IN_W3C_CHUNKED -> () -> {
                int contentLength = requestHeaders.decodedContentLength()
                        .orElseThrow(() -> new WebApplicationException(BAD_REQUEST));
                return Optional.of(contentLength);
            };

            default -> Optional::empty;
        };

        return new RequestContent()
        {
            @Override
            public Optional<Integer> contentLength()
            {
                return contentLengthSupplier.get();
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
}
