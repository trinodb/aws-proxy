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
import io.trino.aws.proxy.server.signing.SigningQueryParameters;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestContent.ContentType;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.trino.aws.proxy.server.signing.SigningQueryParameters.splitQueryParameters;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;

class RequestBuilder
{
    private static final Logger log = Logger.get(RequestBuilder.class);

    private RequestBuilder() {}

    static Request fromRequest(ContainerRequest request)
    {
        MultiMap requestHeaders = ImmutableMultiMap.copyOfCaseInsensitive(request.getHeaders().entrySet());
        Optional<Instant> requestTimestamp;

        RequestContent requestContent = request.hasEntity() ? buildRequestContent(request.getEntityStream(), requestHeaders) : RequestContent.EMPTY;
        SigningQueryParameters signingQueryParameters = splitQueryParameters(ImmutableMultiMap.copyOf(request.getUriInfo().getQueryParameters(true).entrySet()));

        Optional<RequestAuthorization> requestAuthorization = requestHeaders.getFirst("authorization")
                .map(authorizationHeader -> RequestAuthorization.parse(authorizationHeader, requestHeaders.getFirst("x-amz-security-token")));
        if (requestAuthorization.isPresent()) {
            requestTimestamp = requestHeaders.getFirst("x-amz-date")
                    .map(AwsTimestamp::fromRequestTimestamp);
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
                requestHeaders,
                signingQueryParameters.passthroughQueryParameters(),
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
    private static RequestContent buildRequestContent(InputStream requestEntityStream, MultiMap requestHeaders)
    {
        ContentType contentType = getRequestContentTypeFromHeaders(requestHeaders);

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

            case AWS_CHUNKED -> () -> {
                int contentLength = requestHeaders.getFirst("x-amz-decoded-content-length")
                        .map(header -> {
                            try {
                                return Integer.parseInt(header);
                            }
                            catch (NumberFormatException e) {
                                throw new WebApplicationException(e, BAD_REQUEST);
                            }
                        })
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

    private static Optional<ContentType> getChunkingType(String valueToParse)
    {
        return switch (valueToParse.toLowerCase(Locale.ROOT).trim()) {
            case "aws-chunked" -> Optional.of(ContentType.AWS_CHUNKED);
            case "chunked" -> Optional.of(ContentType.W3C_CHUNKED);
            default -> Optional.empty();
        };
    }

    private static ContentType getRequestContentTypeFromHeaders(MultiMap requestHeaders)
    {
        List<String> requestContentEncoding = requestHeaders.get("content-encoding");
        if (requestContentEncoding.isEmpty()) {
            return getChunkingType(requestHeaders.getFirst("transfer-encoding").orElse("")).orElse(ContentType.STANDARD);
        }
        // As per RFC9110 Section 5.3.1:
        // Multiple content encoding headers may be sent
        // Or alternatively a single one may contain multiple comma-separated values
        List<ContentType> allContentEncodings = requestContentEncoding.stream()
                .flatMap(item -> Splitter.on(",").splitToStream(item))
                .flatMap(valueToParse -> getChunkingType(valueToParse).stream())
                .collect(toImmutableList());
        return switch (allContentEncodings.size()) {
            case 0 -> ContentType.STANDARD;
            case 1 -> allContentEncodings.getFirst();
            default -> throw new WebApplicationException(BAD_REQUEST);
        };
    }
}
