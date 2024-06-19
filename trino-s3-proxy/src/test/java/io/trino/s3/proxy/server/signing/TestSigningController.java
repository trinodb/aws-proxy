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

import io.airlift.units.Duration;
import io.trino.s3.proxy.server.credentials.CredentialsController;
import io.trino.s3.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.s3.proxy.spi.collections.ImmutableMultiMap;
import io.trino.s3.proxy.spi.credentials.Credential;
import io.trino.s3.proxy.spi.credentials.Credentials;
import io.trino.s3.proxy.spi.credentials.CredentialsProvider;
import io.trino.s3.proxy.spi.signing.SigningController;
import io.trino.s3.proxy.spi.signing.SigningMetadata;
import io.trino.s3.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSigningController
{
    private static final Credentials CREDENTIALS = Credentials.build(new Credential("THIS_IS_AN_ACCESS_KEY", "THIS_IS_A_SECRET_KEY"));

    private final CredentialsProvider credentialsProvider = (emulatedAccessKey, session) -> Optional.of(CREDENTIALS);
    private final CredentialsController credentialsController = new CredentialsController(new TestingRemoteS3Facade(), credentialsProvider);
    private final SigningController signingController = new InternalSigningController(credentialsController, new SigningControllerConfig().setMaxClockDrift(new Duration(99999, TimeUnit.DAYS)));

    @Test
    public void testRootLs()
    {
        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false);
        String xAmzDate = "20240516T024511Z";
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeadersBuilder.putOrReplaceSingle("Host", "localhost:10064");
        requestHeadersBuilder.putOrReplaceSingle("User-Agent", "aws-cli/2.15.16 Python/3.11.7 Darwin/22.6.0 source/x86_64 prompt/off command/s3.ls");
        requestHeadersBuilder.putOrReplaceSingle("Accept-Encoding", "identity");

        String signature = signingController.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "us-east-1",
                xAmzDate,
                Credentials::emulated,
                URI.create("http://localhost:10064/"),
                requestHeadersBuilder.build(),
                ImmutableMultiMap.empty(),
                "GET");

        assertThat(signature).isEqualTo("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=9a19c251bf4e1533174e80da59fa57c65b3149b611ec9a4104f6944767c25704");
    }

    @Test
    public void testRootExpiredClock()
    {
        SigningController signingController = new InternalSigningController(credentialsController, new SigningControllerConfig().setMaxClockDrift(new Duration(1, TimeUnit.MINUTES)));

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false);
        String xAmzDate = "20240516T024511Z";
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeadersBuilder.putOrReplaceSingle("Host", "localhost:10064");
        requestHeadersBuilder.putOrReplaceSingle("User-Agent", "aws-cli/2.15.16 Python/3.11.7 Darwin/22.6.0 source/x86_64 prompt/off command/s3.ls");
        requestHeadersBuilder.putOrReplaceSingle("Accept-Encoding", "identity");

        assertThatThrownBy(() -> signingController.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "us-east-1",
                xAmzDate,
                Credentials::emulated,
                URI.create("http://localhost:10064/"),
                requestHeadersBuilder.build(),
                ImmutableMultiMap.empty(),
                "GET"
        )).isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testBucketLs()
    {
        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        ImmutableMultiMap.Builder requestHeadersBuilder = ImmutableMultiMap.builder(false);
        String xAmzDate = "20240516T034003Z";
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", xAmzDate);
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeadersBuilder.putOrReplaceSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeadersBuilder.putOrReplaceSingle("Host", "localhost:10064");
        requestHeadersBuilder.putOrReplaceSingle("User-Agent", "aws-cli/2.15.16 Python/3.11.7 Darwin/22.6.0 source/x86_64 prompt/off command/s3.ls");
        requestHeadersBuilder.putOrReplaceSingle("Accept-Encoding", "identity");

        ImmutableMultiMap.Builder queryParametersBuilder = ImmutableMultiMap.builder(true);
        queryParametersBuilder.putOrReplaceSingle("list-type", "2");
        queryParametersBuilder.putOrReplaceSingle("prefix", "foo/bar");
        queryParametersBuilder.putOrReplaceSingle("delimiter", "/");
        queryParametersBuilder.putOrReplaceSingle("encoding-type", "url");

        String signature = signingController.signRequest(
                new SigningMetadata(SigningServiceType.S3, CREDENTIALS, Optional.empty()),
                "us-east-1",
                xAmzDate,
                Credentials::emulated,
                URI.create("http://localhost:10064/mybucket"),
                requestHeadersBuilder.build(),
                queryParametersBuilder.build(),
                "GET");

        assertThat(signature).isEqualTo("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=222d7b7fcd4d5560c944e8fecd9424ee3915d131c3ad9e000d65db93e87946c4");
    }
}
