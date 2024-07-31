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
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRequestHeadersBuilder
{
    private static final String SAMPLE_AUTHORIZATION_HEADER = "AWS4-HMAC-SHA256 Credential=dummy/20240730/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef";
    private static final RequestAuthorization SAMPLE_PARSED_AUTHORIZATION = new RequestAuthorization("dummy", "us-east-1", "20240730/us-east-1/s3/aws4_request", ImmutableSet.of("host", "x-amz-content-sha256", "x-amz-date"), "abcdef", Optional.empty(), Optional.empty());

    @Test
    public void testBuildHeaders()
    {
        testBuildHeaders(ImmutableMultiMap.empty());
        testBuildHeaders(ImmutableMultiMap.builder(false).add("content-encoding", "gzip").build());
        testBuildHeaders(ImmutableMultiMap.builder(false)
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
                        .build());
    }

    private void testBuildHeaders(MultiMap extraHeaders)
    {
        testBuildHeaders(extraHeaders, extraHeaders);
    }

    private void testBuildHeaders(MultiMap extraHeaders, MultiMap expectedForwardableHeaders)
    {
        String requestTimestampStr = "20240730T010203Z";
        ImmutableMultiMap.Builder rawHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", requestTimestampStr)
                .add("X-Amz-Content-SHA256", "abcdef")
                .add("Content-Length", "1234");
        extraHeaders.forEach(rawHeadersBuilder::addAll);

        InternalRequestHeaders simpleParsedHeaders = RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build());
        assertThat(simpleParsedHeaders.requestAuthorization()).contains(SAMPLE_PARSED_AUTHORIZATION);
        assertThat(simpleParsedHeaders.requestDate()).contains(AwsTimestamp.fromRequestTimestamp(requestTimestampStr));
        assertThat(simpleParsedHeaders.contentLength()).contains(1234);
        assertThat(simpleParsedHeaders.decodedContentLength()).isEmpty();
        assertThat(simpleParsedHeaders.requestPayloadContentType()).isEmpty();
        assertThat(simpleParsedHeaders.requestHeaders().passthroughHeaders().entrySet()).isEqualTo(expectedForwardableHeaders.entrySet());
        assertThat(simpleParsedHeaders.requestHeaders().unmodifiedHeaders().entrySet()).isEqualTo(rawHeadersBuilder.build().entrySet());
    }

    @Test
    public void testBuildHeadersAwsChunked()
    {
        testBuildHeadersAwsChunked(
                ImmutableMultiMap.builder(false).add("Content-Encoding", "aws-chunked").build(),
                ImmutableMultiMap.empty());
        testBuildHeadersAwsChunked(
                ImmutableMultiMap.builder(false).add("Content-Encoding", "aws-chunked,gzip").build(),
                ImmutableMultiMap.builder(false).add("Content-Encoding", "gzip").build());
        testBuildHeadersAwsChunked(
                ImmutableMultiMap.builder(false)
                        .add("Content-Encoding", "aws-chunked,gzip")
                        .add("Content-Encoding", "compress")
                        .add("X-Amz-Metadata-foo", "bar")
                        .build(),
                ImmutableMultiMap.builder(false)
                        .add("Content-Encoding", "gzip,compress")
                        .add("X-Amz-Metadata-foo", "bar")
                        .build());
    }

    private void testBuildHeadersAwsChunked(MultiMap extraHeaders, MultiMap expectedForwardableHeaders)
    {
        String requestTimestampStr = "20240730T010203Z";
        ImmutableMultiMap.Builder rawHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", requestTimestampStr)
                .add("X-Amz-Content-SHA256", "abcdef")
                .add("Content-Length", "1234")
                .add("X-Amz-Decoded-Content-Length", "1000");
        extraHeaders.forEach(rawHeadersBuilder::addAll);
        InternalRequestHeaders awsChunkedParsedHeaders = RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build());

        assertThat(awsChunkedParsedHeaders.requestAuthorization()).contains(SAMPLE_PARSED_AUTHORIZATION);
        assertThat(awsChunkedParsedHeaders.requestDate()).contains(AwsTimestamp.fromRequestTimestamp(requestTimestampStr));
        assertThat(awsChunkedParsedHeaders.contentLength()).contains(1234);
        assertThat(awsChunkedParsedHeaders.decodedContentLength()).contains(1000);
        assertThat(awsChunkedParsedHeaders.requestPayloadContentType()).contains(RequestContent.ContentType.AWS_CHUNKED);

        assertThat(awsChunkedParsedHeaders.requestHeaders().passthroughHeaders().entrySet()).isEqualTo(expectedForwardableHeaders.entrySet());
        assertThat(awsChunkedParsedHeaders.requestHeaders().unmodifiedHeaders().entrySet()).isEqualTo(rawHeadersBuilder.build().entrySet());
    }

    @Test
    public void testBuildHeadersHttpChunked()
    {
        testBuildHeadersHttpChunked(ImmutableMultiMap.empty());
        testBuildHeadersHttpChunked(
                ImmutableMultiMap.builder(false).add("X-Amz-Some-Metadata", "foo").build());
        testBuildHeadersHttpChunked(
                ImmutableMultiMap.builder(false)
                        .add("X-Amz-Some-Metadata", "foo")
                        .add("Content-Encoding", "compress")
                        .add("Content-Encoding", "gzip").build(),
                ImmutableMultiMap.builder(false)
                        .add("X-Amz-Some-Metadata", "foo")
                        .add("Content-Encoding", "compress,gzip").build());
    }

    private void testBuildHeadersHttpChunked(MultiMap extraHeaders)
    {
        testBuildHeadersHttpChunked(extraHeaders, extraHeaders);
    }

    private void testBuildHeadersHttpChunked(MultiMap extraHeaders, MultiMap expectedForwardableHeaders)
    {
        String requestTimestampStr = "20240730T010203Z";
        ImmutableMultiMap.Builder rawHeadersBuilder = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", requestTimestampStr)
                .add("X-Amz-Content-SHA256", "abcdef")
                .add("Transfer-Encoding", "chunked")
                .add("X-Amz-Decoded-Content-Length", "1000");
        extraHeaders.forEach(rawHeadersBuilder::addAll);
        InternalRequestHeaders httpChunkedParsedHeaders = RequestHeadersBuilder.parseHeaders(rawHeadersBuilder.build());

        assertThat(httpChunkedParsedHeaders.requestAuthorization()).contains(SAMPLE_PARSED_AUTHORIZATION);
        assertThat(httpChunkedParsedHeaders.requestDate()).contains(AwsTimestamp.fromRequestTimestamp(requestTimestampStr));
        assertThat(httpChunkedParsedHeaders.contentLength()).isEmpty();
        assertThat(httpChunkedParsedHeaders.decodedContentLength()).contains(1000);
        assertThat(httpChunkedParsedHeaders.requestPayloadContentType()).contains(RequestContent.ContentType.W3C_CHUNKED);

        assertThat(httpChunkedParsedHeaders.requestHeaders().passthroughHeaders().entrySet()).isEqualTo(expectedForwardableHeaders.entrySet());
        assertThat(httpChunkedParsedHeaders.requestHeaders().unmodifiedHeaders().entrySet()).isEqualTo(rawHeadersBuilder.build().entrySet());
    }

    @Test
    public void testBuildHeadersThrowsIfMultipleChunkingModesEnabled()
    {
        ImmutableMultiMap rawHeaders = ImmutableMultiMap.builder(false)
                .add("Authorization", SAMPLE_AUTHORIZATION_HEADER)
                .add("Host", "localhost:9876")
                .add("X-Amz-Date", "20240730T010203Z")
                .add("X-Amz-Content-SHA256", "abcdef")
                .add("Transfer-Encoding", "chunked")
                .add("Content-Encoding", "aws-chunked")
                .add("X-Amz-Decoded-Content-Length", "1000")
                .build();
        assertThatThrownBy(() -> RequestHeadersBuilder.parseHeaders(rawHeaders)).isInstanceOf(WebApplicationException.class);
    }
}
