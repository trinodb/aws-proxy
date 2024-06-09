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
package io.trino.s3.proxy.server.signing;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.internal.AbstractAwsS3V4Signer;
import software.amazon.awssdk.auth.signer.internal.CopiedAbstractAwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;
import static io.trino.s3.proxy.server.signing.SigningControllerImpl.Mode.UNADJUSTED_HEADERS;

final class Signer
{
    private static final Logger log = Logger.get(Signer.class);

    static final ZoneId ZONE = ZoneId.of("Z");
    static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(ZONE);
    static final DateTimeFormatter RESPONSE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'", Locale.US).withZone(ZONE);

    private static final String OVERRIDE_CONTENT_HASH = "__TRINO__OVERRIDE_CONTENT_HASH__";

    private static final Set<String> BASE_IGNORED_HEADERS = ImmutableSet.of(
            "x-amz-decoded-content-length",
            "x-amzn-trace-id",
            "expect",
            "accept-encoding",
            "authorization",
            "connection");

    private static final Set<String> LEGACY_IGNORED_HEADERS = BASE_IGNORED_HEADERS;
    private static final Set<String> IGNORED_HEADERS = ImmutableSet.<String>builder().addAll(BASE_IGNORED_HEADERS).add("user-agent").build();

    private static final Set<String> LOWERCASE_HEADERS = ImmutableSet.of("content-type");

    private static final AbstractAwsS3V4Signer aws4Signer = new AbstractAwsS3V4Signer()
    {
        @Override
        protected String calculateContentHash(SdkHttpFullRequest.Builder mutableRequest, AwsS3V4SignerParams signerParams, SdkChecksum contentFlexibleChecksum)
        {
            return extractOverrideContentHash(mutableRequest).orElseGet(() -> super.calculateContentHash(mutableRequest, signerParams, contentFlexibleChecksum));
        }
    };

    private static final CopiedAbstractAwsS3V4Signer legacyAws4Signer = new CopiedAbstractAwsS3V4Signer()
    {
        @Override
        protected String calculateContentHash(SdkHttpFullRequest.Builder mutableRequest, AwsS3V4SignerParams signerParams, SdkChecksum contentFlexibleChecksum)
        {
            return extractOverrideContentHash(mutableRequest).orElseGet(() -> super.calculateContentHash(mutableRequest, signerParams, contentFlexibleChecksum));
        }
    };

    private static Optional<String> extractOverrideContentHash(SdkHttpFullRequest.Builder mutableRequest)
    {
        // look for the stashed OVERRIDE_CONTENT_HASH, remove the hacked header and then return the stashed hash value
        return mutableRequest.firstMatchingHeader(OVERRIDE_CONTENT_HASH).map(hash -> {
            mutableRequest.removeHeader(OVERRIDE_CONTENT_HASH);
            return hash;
        });
    }

    private Signer() {}

    static String sign(
            SigningControllerImpl.Mode mode,
            boolean isLegacy,
            boolean signatureHasContentLength,
            String serviceName,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            String region,
            String accessKey,
            String secretKey,
            Duration maxClockDrift,
            Optional<byte[]> entity)
    {
        BiFunction<String, List<String>, List<String>> lowercaseHeaderValues = (key, values) -> {
            // mode == UNADJUSTED_HEADERS is temp until airlift is fixed
            if ((mode == UNADJUSTED_HEADERS) || !LOWERCASE_HEADERS.contains(key)) {
                return values;
            }
            return values.stream().map(value -> value.toLowerCase(Locale.ROOT)).collect(toImmutableList());
        };

        requestHeaders = lowercase(requestHeaders, lowercaseHeaderValues);

        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .uri(requestURI)
                .method(SdkHttpMethod.fromValue(httpMethod));

        entity.ifPresent(entityBytes -> requestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(entityBytes)));

        Set<String> ignoredHeaders = isLegacy ? LEGACY_IGNORED_HEADERS : IGNORED_HEADERS;
        requestHeaders
                .entrySet()
                .stream()
                .filter(entry -> !ignoredHeaders.contains(entry.getKey()))
                .filter(entry -> !entry.getKey().equals("content-length") || signatureHasContentLength)
                .forEach(entry -> entry.getValue().forEach(value -> requestBuilder.appendHeader(entry.getKey(), value)));

        queryParameters.forEach(requestBuilder::putRawQueryParameter);

        Optional<String> maybeAmazonContentHash = Optional.ofNullable(requestHeaders.getFirst("x-amz-content-sha256"));
        boolean enablePayloadSigning = maybeAmazonContentHash
                .map(contentHashHeader -> !contentHashHeader.equals("UNSIGNED-PAYLOAD"))
                .orElse(true);
        if (enablePayloadSigning) {
            maybeAmazonContentHash.ifPresent(contentHashHeader -> {
                if (!contentHashHeader.startsWith("STREAMING-")) {
                    // because we stream content without spooling we want to re-use the provided content hash
                    // so that we don't have to calculate it to validate the incoming signature.
                    // Stash the hash in the OVERRIDE_CONTENT_HASH so that aws4Signer can find it and
                    // return it.
                    requestBuilder.putHeader(OVERRIDE_CONTENT_HASH, contentHashHeader);
                }
            });
        }

        boolean enableChunkedEncoding = Optional.ofNullable(requestHeaders.getFirst("content-encoding"))
                .map(contentHashHeader -> contentHashHeader.equals("aws-chunked"))
                .orElse(false);
        if (enableChunkedEncoding) {
            // when chunked, the correct signature needs to reset the content length to the original decoded length
            Optional.ofNullable(requestHeaders.getFirst("x-amz-decoded-content-length"))
                    .ifPresent(decodedContentLength -> requestBuilder.putHeader("content-length", decodedContentLength));
        }

        AwsS3V4SignerParams.Builder signerParamsBuilder = AwsS3V4SignerParams.builder()
                .signingName(serviceName)
                .signingRegion(Region.of(region))
                .doubleUrlEncode(false)
                .enablePayloadSigning(enablePayloadSigning)
                .enableChunkedEncoding(enableChunkedEncoding)
                .awsCredentials(AwsBasicCredentials.create(accessKey, secretKey));

        String xAmzDate = Optional.ofNullable(requestHeaders.getFirst("x-amz-date")).orElseThrow(() -> {
            log.debug("Missing \"x-amz-date\" header");
            return new WebApplicationException(Response.Status.BAD_REQUEST);
        });
        ZonedDateTime zonedRequestDateTime = ZonedDateTime.parse(xAmzDate, AMZ_DATE_FORMAT);

        ZonedDateTime now = ZonedDateTime.now(zonedRequestDateTime.getZone());
        Duration driftFromNow = Duration.between(now, zonedRequestDateTime).abs();
        if (driftFromNow.compareTo(maxClockDrift) > 0) {
            log.debug("Request time exceeds max drift. RequestTime: %s Now: %s", zonedRequestDateTime, now);
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        // because we're verifying the signature provided we must match the clock that they used
        Clock clock = Clock.fixed(zonedRequestDateTime.toInstant(), zonedRequestDateTime.getZone());
        signerParamsBuilder.signingClockOverride(clock);

        BiFunction<SdkHttpFullRequest, AwsS3V4SignerParams, SdkHttpFullRequest> signingProc = isLegacy ? legacyAws4Signer::sign : aws4Signer::sign;
        SdkHttpFullRequest signedRequest = signingProc.apply(requestBuilder.build(), signerParamsBuilder.build());

        return signedRequest.firstMatchingHeader("Authorization").orElseThrow(() -> {
            log.debug("Signer did not generate \"Authorization\" header");
            return new WebApplicationException(Response.Status.BAD_REQUEST);
        });
    }
}
