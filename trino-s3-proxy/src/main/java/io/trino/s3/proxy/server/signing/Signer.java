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

import io.airlift.log.Logger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
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

import static io.trino.s3.proxy.server.signing.Signers.OVERRIDE_CONTENT_HASH;
import static io.trino.s3.proxy.server.signing.Signers.aws4Signer;
import static io.trino.s3.proxy.server.signing.Signers.legacyAws4Signer;

final class Signer
{
    private static final Logger log = Logger.get(Signer.class);

    static final ZoneId ZONE = ZoneId.of("Z");
    static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(ZONE);
    static final DateTimeFormatter RESPONSE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'", Locale.US).withZone(ZONE);

    private Signer() {}

    static byte[] signingKey(AwsCredentials credentials, Aws4SignerRequestParams signerRequestParams)
    {
        return aws4Signer.signingKey(credentials, signerRequestParams);
    }

    static SigningContext sign(
            String serviceName,
            URI requestURI,
            SigningHeaders signingHeaders,
            MultivaluedMap<String, String> queryParameters,
            String region,
            String requestDate,
            String httpMethod,
            String accessKey,
            String secretKey,
            Duration maxClockDrift,
            Optional<byte[]> entity)
    {
        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .uri(requestURI)
                .method(SdkHttpMethod.fromValue(httpMethod));

        entity.ifPresent(entityBytes -> requestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(entityBytes)));

        signingHeaders.lowercaseHeadersToSign().forEach(entry -> entry.getValue().forEach(value -> requestBuilder.appendHeader(entry.getKey(), value)));

        queryParameters.forEach(requestBuilder::putRawQueryParameter);

        Optional<String> maybeAmazonContentHash = signingHeaders.getFirst("x-amz-content-sha256");
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

        boolean enableChunkedEncoding = signingHeaders.getFirst("content-encoding")
                .map(contentHashHeader -> contentHashHeader.equals("aws-chunked"))
                .orElse(false);
        if (enableChunkedEncoding) {
            // when chunked, the correct signature needs to reset the content length to the original decoded length
            signingHeaders.getFirst("x-amz-decoded-content-length")
                    .ifPresent(decodedContentLength -> requestBuilder.putHeader("content-length", decodedContentLength));
        }

        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsS3V4SignerParams.Builder signerParamsBuilder = AwsS3V4SignerParams.builder()
                .signingName(serviceName)
                .signingRegion(Region.of(region))
                .doubleUrlEncode(false)
                .enablePayloadSigning(enablePayloadSigning)
                .enableChunkedEncoding(enableChunkedEncoding)
                .awsCredentials(credentials);

        ZonedDateTime zonedRequestDateTime = ZonedDateTime.parse(requestDate, AMZ_DATE_FORMAT);

        ZonedDateTime now = ZonedDateTime.now(zonedRequestDateTime.getZone());
        Duration driftFromNow = Duration.between(now, zonedRequestDateTime).abs();
        if (driftFromNow.compareTo(maxClockDrift) > 0) {
            log.debug("Request time exceeds max drift. RequestTime: %s Now: %s", zonedRequestDateTime, now);
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        // because we're verifying the signature provided we must match the clock that they used
        Clock clock = Clock.fixed(zonedRequestDateTime.toInstant(), zonedRequestDateTime.getZone());
        signerParamsBuilder.signingClockOverride(clock);

        SdkHttpFullRequest requestToSign = requestBuilder.build();
        AwsS3V4SignerParams signingParams = signerParamsBuilder.build();

        SdkHttpFullRequest signedRequest = isLegacy(signingHeaders)
                ? legacyAws4Signer.sign(requestToSign, signingParams)
                : aws4Signer.sign(requestToSign, signingParams);

        String authorization = signedRequest.firstMatchingHeader("Authorization").orElseThrow(() -> {
            log.debug("Signer did not generate \"Authorization\" header");
            return new WebApplicationException(Response.Status.BAD_REQUEST);
        });

        return buildSigningContext(authorization, signingKey(credentials, new Aws4SignerRequestParams(signingParams)), requestDate, maybeAmazonContentHash);
    }

    private static SigningContext buildSigningContext(String authorization, byte[] signingKey, String requestDate, Optional<String> contentHash)
    {
        RequestAuthorization requestAuthorization = RequestAuthorization.parse(authorization);
        if (!requestAuthorization.isValid()) {
            // TODO logging, etc.
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
        ChunkSigner chunkSigner = new ChunkSigner(requestDate, requestAuthorization.keyPath(), signingKey);
        ChunkSigningSession chunkSigningSession = new InternalChunkSigningSession(chunkSigner, requestAuthorization.signature());
        return new SigningContext(requestAuthorization, chunkSigningSession, contentHash);
    }

    private static boolean isLegacy(SigningHeaders signingHeaders)
    {
        return signingHeaders.hasHeaderToSign("user-agent");
    }
}
