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
package io.trino.s3.proxy.server;

import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.Credentials.CredentialsEntry;
import io.trino.s3.proxy.server.credentials.CredentialsController;
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningController.Scope;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSigningController
{
    private static final Credentials CREDENTIALS = new Credentials(new CredentialsEntry("THIS_IS_AN_ACCESS_KEY", "THIS_IS_A_SECRET_KEY"));

    private final CredentialsController credentialsController = new CredentialsController()
    {
        @Override
        public Optional<Credentials> credentials(String emulatedAccessKey)
        {
            return Optional.of(CREDENTIALS);
        }

        @Override
        public void upsertCredentials(Credentials credentials)
        {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    public void testRootLs()
    {
        SigningController signingController = new SigningController(credentialsController);

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        MultivaluedMap<String, String> requestHeaders = new MultivaluedHashMap<>();
        requestHeaders.putSingle("X-Amz-Date", "20240516T024511Z");
        requestHeaders.putSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeaders.putSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeaders.putSingle("Host", "localhost:10064");
        requestHeaders.putSingle("User-Agent", "aws-cli/2.15.16 Python/3.11.7 Darwin/22.6.0 source/x86_64 prompt/off command/s3.ls");
        requestHeaders.putSingle("Accept-Encoding", "identity");

        Map<String, String> signedHeaders = signingController.signedRequestHeaders(new Scope("dummy", "THIS_IS_AN_ACCESS_KEY", "us-east-1"), "GET", requestHeaders, "/", "");

        assertThat(signedHeaders).contains(Map.entry("Authorization", "AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=9a19c251bf4e1533174e80da59fa57c65b3149b611ec9a4104f6944767c25704"));
    }

    @Test
    public void testBucketLs()
    {
        SigningController signingController = new SigningController(credentialsController);

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        MultivaluedMap<String, String> requestHeaders = new MultivaluedHashMap<>();
        requestHeaders.putSingle("X-Amz-Date", "20240516T034003Z");
        requestHeaders.putSingle("X-Amz-Content-SHA256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        requestHeaders.putSingle("X-Amz-Security-Token", "FwoGZXIvYXdzEP3//////////wEaDG79rlcAjsgKPP9N3SKIAu7/Zvngne5Ov6kGrDcIIPUZYkGpwNbj8zNnbWgOhiqmOCM3hrk4NuH17mP5n3nC7urlXZxaTCywKpAHpO3YsvLXcwjlfaYFA0Au4oejwSbU9ybIlzPzrqz7lVesgCfJOV+rj5F5UAh19d7RpRpA6Vy4nxGBTTlCNIVbkW9fp2Esql2/vsdh77rAG+j+BQegtegDCKBfen4gHMdvEOF6hyc4ne43eLXjpvUKxBgpI9MjOHtNHrDbOOBFXDDyknoESgE9Hsm12nDuVQhwrI/hhA4YB/MSIpl4FTgVs2sQP3K+v65tmyvIlpL6O78S6spMM9Tv/F4JLtksTzb90w46uZk9sxKC/RBkRijisM6tBjIrr/0znxnW3i5ggGAX4H/Z3aWlxSdzNs2UGWtqig9Plp3Xa9gG+zCKcXmDAA==");
        requestHeaders.putSingle("Host", "localhost:10064");
        requestHeaders.putSingle("User-Agent", "aws-cli/2.15.16 Python/3.11.7 Darwin/22.6.0 source/x86_64 prompt/off command/s3.ls");
        requestHeaders.putSingle("Accept-Encoding", "identity");

        Map<String, String> signedHeaders = signingController.signedRequestHeaders(new Scope("dummy", "THIS_IS_AN_ACCESS_KEY", "us-east-1"), "GET", requestHeaders, "/mybucket", "list-type=2&prefix=foo%2Fbar&delimiter=%2F&encoding-type=url");

        assertThat(signedHeaders).contains(Map.entry("Authorization", "AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240516/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=222d7b7fcd4d5560c944e8fecd9424ee3915d131c3ad9e000d65db93e87946c4"));
    }
}
