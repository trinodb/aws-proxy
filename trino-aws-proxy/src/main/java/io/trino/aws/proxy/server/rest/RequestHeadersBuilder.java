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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.aws.proxy.spi.rest.RequestContent.ContentType;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.server.rest.S3PresignController.PRESIGNED_URL_METHODS_HEADER_NAME;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.LENGTH_REQUIRED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

final class RequestHeadersBuilder
{
    private RequestHeadersBuilder() {}

    private static final Set<String> IGNORED_HEADERS = ImmutableSet.of(
            "x-amzn-trace-id",
            "expect",
            "accept-encoding",
            "user-agent",
            "connection",
            "amz-sdk-invocation-id",
            "amx-sdk-request",
            "host",
            PRESIGNED_URL_METHODS_HEADER_NAME.toLowerCase(ENGLISH));

    record InternalRequestHeaders(
            RequestHeaders requestHeaders,
            Optional<RequestAuthorization> requestAuthorization,
            Optional<Instant> requestDate,
            Optional<Integer> contentLength,
            Optional<Integer> decodedContentLength,
            Optional<ContentType> requestPayloadContentType)
    {
        InternalRequestHeaders {
            requireNonNull(requestHeaders, "requestHeaders is null");
            requireNonNull(requestAuthorization, "requestAuthorization is null");
            requireNonNull(requestDate, "requestDate is null");
            requireNonNull(contentLength, "contentLength is null");
            requireNonNull(decodedContentLength, "decodedContentLength is null");
            requireNonNull(requestPayloadContentType, "requestPayloadContentType is null");
        }
    }

    static InternalRequestHeaders parseHeaders(MultiMap allRequestHeaders)
    {
        Builder builder = new Builder();
        allRequestHeaders.forEach((headerName, headerValues) -> {
            switch (headerName) {
                case "authorization", "x-amz-security-token" -> {} // these get handled separately
                case "content-length" -> builder.contentLength(headerValues);
                case "x-amz-decoded-content-length" -> builder.decodedContentLength(headerValues);
                case "content-encoding" -> builder.contentEncoding(headerValues);
                case "transfer-encoding" -> builder.transferEncoding(headerValues);
                case "x-amz-date" -> builder.requestDate(headerValues);
                case "x-amz-content-sha256" -> builder.contentSha256(headerValues);
                default -> {
                    if (!IGNORED_HEADERS.contains(headerName)) {
                        builder.addPassthroughHeader(headerName, headerValues);
                    }
                }
            }
        });
        allRequestHeaders.getFirst("authorization").map(authorization -> RequestAuthorization.parse(authorization, allRequestHeaders.getFirst("x-amz-security-token"))).ifPresent(builder::requestAuthorization);

        return builder.build(allRequestHeaders);
    }

    private static class Builder
    {
        private final ImmutableMultiMap.Builder passthroughHeadersBuilder = ImmutableMultiMap.builder(false);
        private Optional<RequestAuthorization> requestAuthorization = Optional.empty();
        private Optional<Instant> requestDate = Optional.empty();
        private Optional<Integer> contentLength = Optional.empty();
        private Optional<Integer> decodedContentLength = Optional.empty();
        private Optional<String> contentSha256 = Optional.empty();
        private Set<ContentType> seenRequestPayloadContentTypes = new HashSet<>();

        private Builder() {}

        private static Optional<String> parseHeaderValuesAsSingle(List<String> allValues)
        {
            return parseHeaderValuesAsSingle(allValues, identity());
        }

        private static <T> Optional<T> parseHeaderValuesAsSingle(List<String> allValues, Function<String, T> converter)
        {
            Function<String, T> wrappedConverter = value -> {
                try {
                    return converter.apply(value);
                }
                catch (Exception _) {
                    throw new WebApplicationException(BAD_REQUEST);
                }
            };

            return switch (allValues.size()) {
                case 0 -> Optional.empty();
                case 1 -> Optional.of(wrappedConverter.apply(allValues.getFirst()));
                default -> throw new WebApplicationException(BAD_REQUEST);
            };
        }

        private void requestAuthorization(RequestAuthorization requestAuthorization)
        {
            this.requestAuthorization = Optional.of(requestAuthorization);
        }

        private void requestDate(List<String> values)
        {
            this.requestDate = parseHeaderValuesAsSingle(values, AwsTimestamp::fromRequestTimestamp);
        }

        private void contentLength(List<String> values)
        {
            this.contentLength = parseHeaderValuesAsSingle(values, Integer::parseUnsignedInt);
        }

        private void decodedContentLength(List<String> values)
        {
            this.decodedContentLength = parseHeaderValuesAsSingle(values, Integer::parseUnsignedInt);
        }

        private void contentEncoding(List<String> values)
        {
            // As per RFC9110 Section 5.3.1:
            // Multiple content encoding headers may be sent
            // Or alternatively a single one may contain multiple comma-separated values
            List<String> allContentEncodings = values.stream().flatMap(item -> Splitter.on(",").splitToStream(item)).collect(toImmutableList());
            List<String> forwardableContentEncodings = new LinkedList<>();
            for (String contentEncoding : allContentEncodings) {
                if (contentEncoding.equalsIgnoreCase("aws-chunked")) {
                    requestPayloadContentType(ContentType.AWS_CHUNKED);
                }
                else {
                    forwardableContentEncodings.add(contentEncoding);
                }
            }
            if (!forwardableContentEncodings.isEmpty()) {
                addPassthroughHeader("content-encoding", ImmutableList.of(String.join(",", forwardableContentEncodings)));
            }
        }

        private void transferEncoding(List<String> values)
        {
            parseHeaderValuesAsSingle(values).ifPresent(value -> {
                if (value.equalsIgnoreCase("chunked")) {
                    requestPayloadContentType(ContentType.W3C_CHUNKED);
                }
            });
        }

        private void contentSha256(List<String> values)
        {
            this.contentSha256 = parseHeaderValuesAsSingle(values, identity());
        }

        private void requestPayloadContentType(ContentType value)
        {
            this.seenRequestPayloadContentTypes.add(value);
        }

        private void addPassthroughHeader(String headerName, List<String> headerValues)
        {
            passthroughHeadersBuilder.addAll(headerName, headerValues);
        }

        private void assertContentTypeValid(ContentType actualContentType)
        {
            if (actualContentType == ContentType.AWS_CHUNKED || actualContentType == ContentType.AWS_CHUNKED_IN_W3C_CHUNKED || actualContentType == ContentType.W3C_CHUNKED) {
                if (decodedContentLength.isEmpty()) {
                    throw new WebApplicationException(LENGTH_REQUIRED);
                }
                String sha256 = contentSha256.orElseThrow(() -> new WebApplicationException(BAD_REQUEST));
                if (actualContentType != ContentType.W3C_CHUNKED && !sha256.startsWith("STREAMING-")) {
                    throw new WebApplicationException(BAD_REQUEST);
                }
            }
        }

        private InternalRequestHeaders build(MultiMap allHeaders)
        {
            Optional<ContentType> applicableContentType = switch (seenRequestPayloadContentTypes.size()) {
                case 0, 1 -> seenRequestPayloadContentTypes.stream().findFirst();
                case 2 -> {
                    if (!seenRequestPayloadContentTypes.containsAll(ImmutableSet.of(ContentType.AWS_CHUNKED, ContentType.W3C_CHUNKED))) {
                        throw new WebApplicationException(BAD_REQUEST);
                    }
                    yield Optional.of(ContentType.AWS_CHUNKED_IN_W3C_CHUNKED);
                }
                default -> throw new WebApplicationException(BAD_REQUEST);
            };
            applicableContentType.ifPresent(this::assertContentTypeValid);

            return new InternalRequestHeaders(
                    new RequestHeaders(passthroughHeadersBuilder.build(), allHeaders),
                    requestAuthorization, requestDate, contentLength, decodedContentLength,
                    applicableContentType);
        }
    }
}
