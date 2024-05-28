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
package io.trino.s3.proxy.server.credentials;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
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
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

final class Signer
{
    private static final Logger log = Logger.get(Signer.class);

    static final ZoneId ZONE = ZoneId.of("Z");
    static final ZoneId UTC = ZoneId.of("UTC");
    static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(ZONE);
    static final DateTimeFormatter RESPONSE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'", Locale.US).withZone(UTC);

    private static final Set<String> IGNORED_HEADERS = ImmutableSet.of(
            "x-amzn-trace-id",
            "expect",
            "accept-encoding",
            "authorization",
            "user-agent",
            "connection");

    private static final Set<String> LOWERCASE_HEADERS = ImmutableSet.of("content-type");
    private static final AwsS3V4Signer aws4Signer = AwsS3V4Signer.create();

    private Signer() {}

    @SuppressWarnings("SameParameterValue")
    static String sign(
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
        requestHeaders = lowercase(requestHeaders);
        queryParameters = lowercase(queryParameters);

        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .uri(requestURI)
                .method(SdkHttpMethod.fromValue(httpMethod));

        entity.ifPresent(entityBytes -> requestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(entityBytes)));

        requestHeaders
                .entrySet()
                .stream()
                .filter(entry -> !IGNORED_HEADERS.contains(entry.getKey()))
                .forEach(entry -> entry.getValue().forEach(value -> requestBuilder.appendHeader(entry.getKey(), value)));

        queryParameters.forEach(requestBuilder::putRawQueryParameter);

        boolean enablePayloadSigning = Optional.ofNullable(requestHeaders.getFirst("x-amz-content-sha256"))
                .map(contentHashHeader -> !contentHashHeader.equals("UNSIGNED-PAYLOAD"))
                .orElse(true);

        AwsS3V4SignerParams.Builder signerParamsBuilder = AwsS3V4SignerParams.builder()
                .signingName(serviceName)
                .signingRegion(Region.of(region))
                .doubleUrlEncode(false)
                .enablePayloadSigning(enablePayloadSigning)
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

        return aws4Signer.sign(requestBuilder.build(), signerParamsBuilder.build()).firstMatchingHeader("Authorization").orElseThrow(() -> {
            log.debug("Signer did not generate \"Authorization\" header");
            return new WebApplicationException(Response.Status.BAD_REQUEST);
        });
    }

    private static MultivaluedMap<String, String> lowercase(MultivaluedMap<String, String> map)
    {
        MultivaluedMap<String, String> result = new MultivaluedHashMap<>();
        map.forEach((name, values) -> {
            name = name.toLowerCase(Locale.ROOT);
            if (LOWERCASE_HEADERS.contains(name)) {
                values = values.stream().map(value -> value.toLowerCase(Locale.ROOT)).collect(toImmutableList());
            }
            result.put(name, values);
        });
        return result;
    }
}
