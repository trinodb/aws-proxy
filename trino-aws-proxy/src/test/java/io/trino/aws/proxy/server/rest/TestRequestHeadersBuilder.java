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

import com.google.common.collect.ImmutableSet;
import io.trino.aws.proxy.server.rest.RequestHeadersBuilder.InternalRequestHeaders;
import io.trino.aws.proxy.spi.rest.RequestContent.ContentType;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.aws.proxy.spi.rest.RequestContent.ContentType.W3C_CHUNKED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRequestHeadersBuilder
{
    private static final String SAMPLE_AUTHORIZATION_HEADER = "AWS4-HMAC-SHA256 Credential=dummy/20240730/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef";
    private static final RequestAuthorization SAMPLE_PARSED_AUTHORIZATION = new RequestAuthorization("dummy", "us-east-1", "20240730/us-east-1/s3/aws4_request", ImmutableSet.of("host", "x-amz-content-sha256", "x-amz-date"), "abcdef", Optional.empty(), Optional.empty());
    private static final String SAMPLE_TIMESTAMP = "20240730T010203Z";

    @Test
    public void testBuildHeadersNoSpecialContent()
    {
        testBuildHeadersNoSpecialContent(ImmutableMultiMap.empty());
        testBuildHeadersNoSpecialContent(ImmutableMultiMap.builder(false).add("content-encoding", "gzip").build());
        testBuildHeadersNoSpecialContent(ImmutableMultiMap.builder(false)
                .add("content-encoding", "gzip")
                .add("x-amz-metadata-foo", "bar")
                .add("test", "header")
                .build());
        // Multiple content encoding headers should be turned into a single comma separated string
        testBuildHeaders(
                ImmutableMultiMap.builder(false)
                        .add("content-encoding", "gzip")
                        .add("content-encoding", "compress")
                        .add("x-amz-metadata-foo", "bar")
                        .build(),
                ImmutableMultiMap.builder(false)
                        .add("content-encoding", "gzip,compress")
                        .add("x-amz-metadata-foo", "bar")
                        .build(),
                Optional.empty());
    }

    private static void testBuildHeadersNoSpecialContent(MultiMap extraHeaders)
    {
        testBuildHeaders(extraHeaders, extraHeaders, Optional.empty());
    }

    @Test
    public void testBuildHeadersAwsChunked()
    {
        testBuildHeadersAwsChunkedPayload(
                ImmutableMultiMap.builder(false).add("Content-Length", "1234").add("X-Amz-Decoded-Content-Length", "1234").build(),
                ContentType.AWS_CHUNKED);
    }

    @Test
    public void testBuildHeadersHttpAndAwsChunked()
    {
        testBuildHeadersAwsChunkedPayload(
                ImmutableMultiMap.builder(false).add("Transfer-Encoding", "chunked").add("X-Amz-Decoded-Content-Length", "1234").build(),
                ContentType.AWS_CHUNKED_IN_W3C_CHUNKED);
    }

    private void testBuildHeadersAwsChunkedPayload(MultiMap baseHeaders, ContentType expectedContentType)
    {
        assertThatThrownBy(() -> doBuildHeaders(mergeMaps(baseHeaders, ImmutableMultiMap.builder(false)
                        .add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
                        .add("Content-Encoding", "aws-chunked")
                        .build()))).isInstanceOf(WebApplicationException.class);

        testBuildHeaders(
                mergeMaps(
                        baseHeaders,
                        ImmutableMultiMap.builder(false)
                                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                                .add("Content-Encoding", "aws-chunked").build()),
                ImmutableMultiMap.empty(),
                Optional.of(expectedContentType));

        testBuildHeaders(
                mergeMaps(
                        baseHeaders,
                        ImmutableMultiMap.builder(false)
                                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                                .add("Content-Encoding", "aws-chunked,gzip").build()),
                ImmutableMultiMap.builder(false).add("Content-Encoding", "gzip").build(),
                Optional.of(expectedContentType));

        testBuildHeaders(
                mergeMaps(
                        baseHeaders,
                        ImmutableMultiMap.builder(false)
                                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                                .add("Content-Encoding", "aws-chunked,gzip")
                                .add("Content-Encoding", "compress")
                                .add("X-Amz-Metadata-foo", "bar")
                                .build()),
                ImmutableMultiMap.builder(false)
                        .add("Content-Encoding", "gzip,compress")
                        .add("X-Amz-Metadata-foo", "bar")
                        .build(),
                Optional.of(expectedContentType));
    }

    @Test
    public void testBuildHeadersHttpChunked()
    {
        MultiMap baseHttpChunkedHeaders = ImmutableMultiMap.builder(false)
                .add("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("Transfer-Encoding", "chunked")
                .add("X-Amz-Decoded-Content-Length", "1000")
                .build();
        testBuildHeaders(baseHttpChunkedHeaders, ImmutableMultiMap.empty(), Optional.of(W3C_CHUNKED));

        MultiMap metadataHeaders = ImmutableMultiMap.builder(false).add("X-Amz-Some-Metadata", "foo").build();
        testBuildHeaders(
                mergeMaps(baseHttpChunkedHeaders, metadataHeaders),
                metadataHeaders,
                Optional.of(W3C_CHUNKED));

        testBuildHeaders(
                mergeMaps(
                        baseHttpChunkedHeaders,
                        ImmutableMultiMap.builder(false)
                                .add("X-Amz-Some-Metadata", "foo")
                                .add("Content-Encoding", "compress")
                                .add("Content-Encoding", "gzip").build()),
                ImmutableMultiMap.builder(false)
                        .add("X-Amz-Some-Metadata", "foo")
                        .add("Content-Encoding", "compress,gzip").build(),
                Optional.of(W3C_CHUNKED));
    }

    private static MultiMap mergeMaps(MultiMap base, MultiMap entriesToAdd)
    {
        ImmutableMultiMap.Builder builder = ImmutableMultiMap.builder(false);
        base.forEach(builder::addAll);
        entriesToAdd.forEach(builder::addAll);
        return builder.build();
    }

    private static InternalRequestHeaders doBuildHeaders(MultiMap extraHeaders)
    {
        ImmutableMultiMap.Builder rawHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", SAMPLE_TIMESTAMP);
        extraHeaders.forEach(rawHeadersBuilder::addAll);
        InternalRequestHeaders result = RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build());
        if (!result.requestHeaders().unmodifiedHeaders().entrySet().equals(rawHeadersBuilder.build().entrySet())) {
            throw new IllegalStateException("Unexpected headers");
        }
        return result;
    }

    private static void testBuildHeaders(MultiMap extraHeaders, MultiMap expectedForwardableHeaders, Optional<ContentType> expectedContentType)
    {
        InternalRequestHeaders constructedHeaders = doBuildHeaders(extraHeaders);

        Optional<Integer> contentLength = extraHeaders.getFirst("Content-Length").map(Integer::parseInt);
        Optional<Integer> decodedContentLength = extraHeaders.getFirst("X-Amz-Decoded-Content-Length").map(Integer::parseInt);
        assertThat(constructedHeaders.requestAuthorization()).contains(SAMPLE_PARSED_AUTHORIZATION);
        assertThat(constructedHeaders.requestDate()).contains(AwsTimestamp.fromRequestTimestamp(SAMPLE_TIMESTAMP));
        assertThat(constructedHeaders.contentLength()).isEqualTo(contentLength);
        assertThat(constructedHeaders.decodedContentLength()).isEqualTo(decodedContentLength);
        assertThat(constructedHeaders.requestPayloadContentType()).isEqualTo(expectedContentType);

        assertThat(constructedHeaders.requestHeaders().passthroughHeaders().entrySet()).isEqualTo(expectedForwardableHeaders.entrySet());
    }

    @Test
    public void testBuildHeadersThrowsOnDuplicatedHeaders()
    {
        testBuildHeadersThrowsOnIllegalHeader("X-Amz-Date", "20240101T000000Z", false);
        testBuildHeadersThrowsOnIllegalHeader("Transfer-Encoding", "identity", false);
        testBuildHeadersThrowsOnIllegalHeader("X-Amz-Decoded-Content-Length", "9999", false);
        testBuildHeadersThrowsOnIllegalHeader("Content-Length", "9999", false);
    }

    @Test
    public void testBuildHeadersThrowsOnUnparseableHeaders()
    {
        testBuildHeadersThrowsOnIllegalHeader("X-Amz-Date", "not a date", true);
        testBuildHeadersThrowsOnIllegalHeader("X-Amz-Decoded-Content-Length", "not a number", true);
        testBuildHeadersThrowsOnIllegalHeader("Content-Length", "not a number", true);
    }

    private void testBuildHeadersThrowsOnIllegalHeader(String extraHeader, String extraHeaderValue, boolean replaceHeader)
    {
        ImmutableMultiMap.Builder rawHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", "20240730T010203Z")
                .add("X-Amz-Content-SHA256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .add("Transfer-Encoding", "chunked")
                .add("Content-Encoding", "aws-chunked")
                .add("Content-Length", "1234")
                .add("X-Amz-Decoded-Content-Length", "1000");

        // Sanity check - the base headers are otherwise valid
        RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build());

        if (replaceHeader) {
            rawHeadersBuilder.putOrReplaceSingle(extraHeader, extraHeaderValue);
        }
        else {
            rawHeadersBuilder.add(extraHeader, extraHeaderValue);
        }
        assertThatThrownBy(() -> RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build())).isInstanceOf(WebApplicationException.class);
    }
}
