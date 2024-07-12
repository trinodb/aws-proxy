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

import com.google.common.collect.ImmutableSet;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static io.trino.aws.proxy.spi.signing.RequestAuthorization.parse;
import static io.trino.aws.proxy.spi.signing.RequestAuthorization.presignedParse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRequestAuthorization
{
    private static final String VALID_SIGNATURE_ALGORITHM = "AWS4-HMAC-SHA256";

    @Test
    public void testValues()
    {
        assertThat(parse("")).isEqualTo(new RequestAuthorization("", "", "", ImmutableSet.of(), "", Optional.empty(), Optional.empty()));
        assertThat(parse("").isValid()).isFalse();
        assertThat(parse("").securityToken()).isEmpty();

        String authorizationHeader = "%s Credential=THIS_IS_AN_ACCESS_KEY/20240608/us-east-1/s3/aws4_request, SignedHeaders=AMZ-SDK-INVOCATION-ID;amz-sdk-request;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date, Signature=c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd".formatted(VALID_SIGNATURE_ALGORITHM);
        RequestAuthorization requestAuthorization = parse(authorizationHeader);
        assertThat(requestAuthorization.isValid()).isTrue();
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("THIS_IS_AN_ACCESS_KEY", "us-east-1");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactly("amz-sdk-invocation-id", "amz-sdk-request", "amz-sdk-retry", "content-type", "host", "user-agent", "x-amz-content-sha256", "x-amz-date");
        assertThat(requestAuthorization.signature()).isEqualTo("c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd");
        assertThat(requestAuthorization.securityToken()).isEmpty();
        assertThat(requestAuthorization.authorization().toLowerCase(Locale.ROOT)).isEqualTo(authorizationHeader.toLowerCase(Locale.ROOT));
        assertThat(requestAuthorization.expiry()).isEmpty();

        RequestAuthorization invalidRequestAuthorization = parse(authorizationHeader.replaceAll(VALID_SIGNATURE_ALGORITHM, "SOME-OTHER-ALGORITHM"));
        assertThat(invalidRequestAuthorization.isValid()).isFalse();

        requestAuthorization = parse("");
        assertThat(requestAuthorization.isValid()).isFalse();
        assertThat(requestAuthorization.lowercaseSignedHeaders()).isEmpty();
        assertThat(requestAuthorization.signature()).isEmpty();
        assertThat(requestAuthorization.securityToken()).isEmpty();

        requestAuthorization = parse("x=y,,,,,,SignedHeaders=");
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("", "");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).isEmpty();
        assertThat(requestAuthorization.signature()).isEmpty();
        assertThat(requestAuthorization.securityToken()).isEmpty();

        requestAuthorization = parse("x=y,,,,,,SignedHeaders=b;b;b;b;c,,,,,,,,,", Optional.of("some-token"));
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("", "");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactly("b", "c");
        assertThat(requestAuthorization.signature()).isEmpty();
        assertThat(requestAuthorization.securityToken()).contains("some-token");

        requestAuthorization = parse("%s Credential=x/y/z,SignedHeaders=1,Signature=foo".formatted(VALID_SIGNATURE_ALGORITHM), Optional.of("some-token"));
        assertThat(requestAuthorization.isValid()).isTrue();
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("x", "z");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactly("1");
        assertThat(requestAuthorization.signature()).isEqualTo("foo");
        assertThat(requestAuthorization.securityToken()).contains("some-token");
    }

    @Test
    public void testPresignedAuthorization()
    {
        String accessKey = "THIS_IS_AN_ACCESS_KEY";
        String region = "us-east-1";
        String credential = "%s/20240608/%s/s3/aws4_request".formatted(accessKey, region);

        testPresignedAuthorizationValid(
                "some-access-key",
                "some-region",
                "foo",
                "some-signature",
                123L,
                Optional.empty(),
                ImmutableSet.of("foo"));
        testPresignedAuthorizationValid(
                "some-access-key",
                "some-region",
                "SOME;header;header;header",
                "some-signature",
                999999L,
                Optional.of("some-token"),
                ImmutableSet.of("some", "header"));

        assertThat(presignedParse(VALID_SIGNATURE_ALGORITHM, credential, "FOO;BAR", "some-signature", -123, Instant.now(), Optional.of("some-token"))
                .isValid()).isFalse();
        assertThat(presignedParse(VALID_SIGNATURE_ALGORITHM, credential, "FOO;BAR", "some-signature", 0, Instant.now(), Optional.of("some-token"))
                .isValid()).isFalse();
        assertThat(presignedParse("SOME-OTHER-ALGORITHM", credential, "FOO;BAR", "some-signature", 123, Instant.now(), Optional.of("some-token"))
                .isValid()).isFalse();
    }

    private void testPresignedAuthorizationValid(String accessKey, String region, String signedHeaders, String signature, Long expirySeconds, Optional<String> securityToken, Set<String> expectedSignedHeaders)
    {
        Instant baseTimestamp = Instant.now();
        String credential = "%s/20240608/%s/s3/aws4_request".formatted(accessKey, region);

        RequestAuthorization requestAuthorization = presignedParse(
                VALID_SIGNATURE_ALGORITHM,
                credential,
                signedHeaders,
                signature,
                expirySeconds,
                baseTimestamp,
                securityToken);
        assertThat(requestAuthorization.isValid()).isTrue();
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region, RequestAuthorization::signature)
                .containsExactly(accessKey, region, signature);
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactlyElementsOf(expectedSignedHeaders);
        assertThat(requestAuthorization.securityToken()).isEqualTo(securityToken);
        assertThat(requestAuthorization.expiry()).contains(baseTimestamp.plusSeconds(expirySeconds));
        assertThatThrownBy(requestAuthorization::authorization).isInstanceOf(IllegalStateException.class).hasMessage("authorization cannot be computed for an expiring request");
    }

    @Test
    public void testPresignedAuthorizationExpiry()
    {
        Instant oneMinuteAgo = Instant.now().minusSeconds(60);
        RequestAuthorization expiredAuthorization = presignedParse(VALID_SIGNATURE_ALGORITHM, "some-credential/20240608/some-region/s3/aws4_request", "FOO", "some-signature", 10, oneMinuteAgo, Optional.empty());
        assertThat(expiredAuthorization.isValid()).isFalse();
        assertThat(expiredAuthorization.expiry()).get().matches(expiry -> expiry.isBefore(Instant.now()));

        RequestAuthorization validAuthorization = presignedParse(VALID_SIGNATURE_ALGORITHM, "some-credential/20240608/some-region/s3/aws4_request", "FOO", "some-signature", 100, oneMinuteAgo, Optional.empty());
        assertThat(validAuthorization.isValid()).isTrue();
        assertThat(validAuthorization.expiry()).get().matches(expiry -> !expiry.isBefore(Instant.now()));
    }
}
