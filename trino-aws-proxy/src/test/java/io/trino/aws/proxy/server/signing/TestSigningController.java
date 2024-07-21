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
package io.trino.aws.proxy.server.signing;

import io.airlift.units.Duration;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.credentials.CredentialsController;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSigningController
{
    private static final Credentials CREDENTIALS = Credentials.build(new Credential("THIS_IS_AN_ACCESS_KEY", "THIS_IS_A_SECRET_KEY"));
    private static final CredentialsProvider CREDENTIALS_PROVIDER = (_, _) -> Optional.of(CREDENTIALS);
    private static final CredentialsController CREDENTIALS_CONTROLLER = new CredentialsController(new TestingRemoteS3Facade(), CREDENTIALS_PROVIDER);
    private static final SigningController LARGE_DRIFT_SIGNING_CONTROLLER = new InternalSigningController(CREDENTIALS_CONTROLLER, new SigningControllerConfig().setMaxClockDrift(new Duration(99999, TimeUnit.DAYS)), new RequestLoggerController(new TrinoAwsProxyConfig()));

    @Test
    public void testRootLs()
    {
        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false);
        String xAmzDate = "20240516T024511Z";
        Instant parsedXAmzDate = AwsTimestamp.fromRequestTimestamp(xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeadersBuilder.putOrReplaceSingle("Host", "localhost:10064");

        String signature = LARGE_DRIFT_SIGNING_CONTROLLER.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "us-east-1",
                parsedXAmzDate,
                Optional.empty(),
                Credentials::emulated,
                URI.create("http://localhost:10064/"),
                requestHeadersBuilder.build(),
                ImmutableMultiMap.empty(),
                "GET").signingAuthorization().authorization();

        assertThat(signature).isEqualTo("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=9a19c251bf4e1533174e80da59fa57c65b3149b611ec9a4104f6944767c25704");
    }

    @Test
    public void testBucketLs()
    {
        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false);
        String xAmzDate = "20240516T034003Z";
        Instant parsedXAmzDate = AwsTimestamp.fromRequestTimestamp(xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeadersBuilder.putOrReplaceSingle("Host", "localhost:10064");

        ImmutableMultiMap.Builder queryParametersBuilder = ImmutableMultiMap.builder(true);
        queryParametersBuilder.putOrReplaceSingle("list-type", "2");
        queryParametersBuilder.putOrReplaceSingle("prefix", "foo/bar");
        queryParametersBuilder.putOrReplaceSingle("delimiter", "/");
        queryParametersBuilder.putOrReplaceSingle("encoding-type", "url");

        String signature = LARGE_DRIFT_SIGNING_CONTROLLER.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "us-east-1",
                parsedXAmzDate,
                Optional.empty(),
                Credentials::emulated,
                URI.create("http://localhost:10064/mybucket"),
                requestHeadersBuilder.build(),
                queryParametersBuilder.build(),
                "GET").signingAuthorization().authorization();

        assertThat(signature).isEqualTo("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=222d7b7fcd4d5560c944e8fecd9424ee3915d131c3ad9e000d65db93e87946c4");
    }

    @Test
    public void testStandardRequestClockDrift()
    {
        long maxClockDriftSeconds = 120;
        Duration maxClockDrift = new Duration(maxClockDriftSeconds, TimeUnit.SECONDS);

        // OK: Recent request
        tryValidateRequestOfAge(Instant.now(), maxClockDrift);
        // OK: Request with a timestamp in the past but within the acceptable drift bounds
        tryValidateRequestOfAge(nowOffsetBySeconds(-(maxClockDriftSeconds - 10)), maxClockDrift);
        // OK: Request with a timestamp in the future but within the acceptable drift bounds
        tryValidateRequestOfAge(nowOffsetBySeconds(maxClockDriftSeconds - 10), maxClockDrift);
        // INVALID: Request with a timestamp in the past beyond the acceptable drift bounds
        assertThatThrownBy(() -> tryValidateRequestOfAge(nowOffsetBySeconds(-(maxClockDriftSeconds + 10)), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);
        // INVALID: Request with a timestamp in the future beyond the acceptable drift bounds
        assertThatThrownBy(() -> tryValidateRequestOfAge(nowOffsetBySeconds(maxClockDriftSeconds + 10), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testPresignedRequestClockDrift()
    {
        long maxClockDriftSeconds = 120;
        Duration maxClockDrift = new Duration(maxClockDriftSeconds, TimeUnit.SECONDS);

        // OK: Request with a recent timestamp and an expiry in the future
        tryValidateRequestOfAgeAndExpiry(Instant.now(), nowOffsetBySeconds(10), maxClockDrift);
        // OK: Request with a timestamp in the future but within acceptable clock drift bounds
        tryValidateRequestOfAgeAndExpiry(nowOffsetBySeconds(maxClockDriftSeconds - 10), nowOffsetBySeconds(maxClockDriftSeconds + 50), maxClockDrift);
        // INVALID: Request with a timestamp in the future beyond acceptable clock drift bounds
        assertThatThrownBy(() -> tryValidateRequestOfAgeAndExpiry(nowOffsetBySeconds(maxClockDriftSeconds + 10), nowOffsetBySeconds(maxClockDriftSeconds + 50), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);

        // OK: Unlike standard requests, the timestamp of the request can be much older than the max clock drift
        tryValidateRequestOfAgeAndExpiry(nowOffsetBySeconds(-(maxClockDriftSeconds * 2)), nowOffsetBySeconds(10), maxClockDrift);

        // The maximum length of a presigned request is 7 days - create a request that was:
        // - Sent 6 days, 23 hours, 59 minutes and 30 seconds ago
        // - Expires in 10 seconds
        // OK: Request is close to the maximum allowable expiration range but still within bounds
        Instant sevenDaysAgo = Instant.now().minus(Signer.MAX_PRESIGNED_REQUEST_AGE);
        Instant almostSevenDaysAgo = sevenDaysAgo.plusSeconds(30);
        tryValidateRequestOfAgeAndExpiry(almostSevenDaysAgo, nowOffsetBySeconds(10), maxClockDrift);

        // INVALID: Old request expired
        assertThatThrownBy(() -> tryValidateRequestOfAgeAndExpiry(sevenDaysAgo, nowOffsetBySeconds(-10), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);
        // INVALID: Recent request expired
        assertThatThrownBy(() -> tryValidateRequestOfAgeAndExpiry(nowOffsetBySeconds(-30), nowOffsetBySeconds(-10), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);

        // INVALID: Request would not have expired, but the time between request timestamp and expiry is too large
        assertThatThrownBy(() -> tryValidateRequestOfAgeAndExpiry(sevenDaysAgo.minusSeconds(100), nowOffsetBySeconds(10), maxClockDrift))
                .isInstanceOf(WebApplicationException.class);
    }

    private static void tryValidateRequestOfAge(Instant requestDate, Duration maxClockDrift)
    {
        tryValidateRequestOfAgeAndExpiry(requestDate, Optional.empty(), maxClockDrift);
    }

    private static void tryValidateRequestOfAgeAndExpiry(Instant requestDate, Instant requestExpiry, Duration maxClockDrift)
    {
        tryValidateRequestOfAgeAndExpiry(requestDate, Optional.of(requestExpiry), maxClockDrift);
    }

    private static void tryValidateRequestOfAgeAndExpiry(Instant requestDate, Optional<Instant> requestExpiry, Duration maxClockDrift)
    {
        RequestLoggerController requestLoggerController = new RequestLoggerController(new TrinoAwsProxyConfig());
        SigningController requestSigningController = new InternalSigningController(CREDENTIALS_CONTROLLER, new SigningControllerConfig().setMaxClockDrift(maxClockDrift), requestLoggerController);

        URI requestUri = URI.create("http://dummy-url");
        MultiMap requestHeaderValues = ImmutableMultiMap.builder(false).putOrReplaceSingle("Host", "http://127.0.0.1:8888").build();
        RequestHeaders requestHeaders = new RequestHeaders(requestHeaderValues, requestHeaderValues);
        MultiMap requestQueryParams = ImmutableMultiMap.empty();

        RequestAuthorization authorization = requestSigningController.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "some-region",
                requestDate,
                requestExpiry,
                Credentials::emulated,
                requestUri,
                requestHeaderValues,
                requestQueryParams,
                "POST").signingAuthorization();
        Request requestToValidate = new Request(UUID.randomUUID(), authorization, requestDate, requestUri, requestHeaders, requestQueryParams, "POST", RequestContent.EMPTY);
        requestLoggerController.newRequestSession(requestToValidate, SigningServiceType.S3);
        requestSigningController.validateAndParseAuthorization(requestToValidate, SigningServiceType.S3);
    }

    private static Instant nowOffsetBySeconds(long seconds)
    {
        return Instant.now().plusSeconds(seconds);
    }
}
