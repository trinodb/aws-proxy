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
package io.trino.aws.proxy.spi.signing;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRequestAuthorization
{
    private static final String ACCESS_KEY = "AKIAEXAMPLEACCESSKEY";
    private static final String KEY_PATH = "20260101/us-east-1/s3/aws4_request";
    private static final String SIGNATURE = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String SECURITY_TOKEN = "FwoGZXIvYXdzEXAMPLESESSIONTOKENVALUE1234567890";

    @Test
    public void testToStringRedactsSecrets()
    {
        RequestAuthorization authorization = authorization(ACCESS_KEY, SIGNATURE, Optional.of(SECURITY_TOKEN));
        String value = authorization.toString();

        assertThat(value)
                .startsWith("RequestAuthorization[")
                .doesNotContain(ACCESS_KEY)
                .doesNotContain(SIGNATURE)
                .doesNotContain(SECURITY_TOKEN)
                .doesNotContain(SECURITY_TOKEN.substring(0, 8))
                .contains("accessKey=****SKEY")
                .contains("signature=0123...cdef(len=64)")
                .contains("securityToken=<redacted,len=%s,sha256=".formatted(SECURITY_TOKEN.length()))
                .contains("region=us-east-1")
                .contains("keyPath=" + KEY_PATH)
                .contains("host")
                .contains("x-amz-date")
                .contains("expiry=Optional.empty");
    }

    @Test
    public void testToStringFieldOrder()
    {
        String value = authorization(ACCESS_KEY, SIGNATURE, Optional.empty()).toString();

        assertThat(value).matches("RequestAuthorization\\[accessKey=.*, region=.*, keyPath=.*, lowercaseSignedHeaders=.*, signature=.*, expiry=.*, securityToken=.*]");
    }

    @Test
    public void testEmptySecurityToken()
    {
        assertThat(authorization(ACCESS_KEY, SIGNATURE, Optional.empty()).toString())
                .contains("securityToken=Optional.empty");
    }

    @Test
    public void testShortValues()
    {
        String value = authorization("AKIA", "abcd", Optional.of("tok")).toString();

        assertThat(value)
                .contains("accessKey=****,")
                .contains("signature=<redacted,len=4>")
                .contains("securityToken=<redacted,len=3,sha256=")
                .doesNotContain("AKIA")
                .doesNotContain("abcd")
                .doesNotContain("tok>");

        assertThat(authorization("12345678", "12345678", Optional.of("12345678")).toString())
                .doesNotContain("1234")
                .doesNotContain("5678");

        assertThat(RequestAuthorization.INVALID.toString())
                .contains("accessKey=****")
                .contains("signature=<redacted,len=0>")
                .contains("securityToken=Optional.empty");
    }

    @Test
    public void testSameTokenRendersSameHash()
    {
        String first = authorization(ACCESS_KEY, SIGNATURE, Optional.of(SECURITY_TOKEN)).toString();
        String second = authorization("AKIAOTHERACCESSKEY99", SIGNATURE.replace('0', 'f'), Optional.of(SECURITY_TOKEN)).toString();
        String other = authorization(ACCESS_KEY, SIGNATURE, Optional.of(SECURITY_TOKEN + "x")).toString();

        assertThat(tokenPart(first)).isEqualTo(tokenPart(second));
        assertThat(tokenPart(first)).isNotEqualTo(tokenPart(other));
        assertThat(tokenPart(first)).matches("securityToken=<redacted,len=\\d+,sha256=[0-9a-f]{8}>]");
    }

    @Test
    public void testEqualityUnchanged()
    {
        Optional<Instant> expiry = Optional.of(Instant.parse("2026-01-01T00:00:00Z"));
        RequestAuthorization first = new RequestAuthorization(ACCESS_KEY, "us-east-1", KEY_PATH, ImmutableSet.of("host"), SIGNATURE, expiry, Optional.of(SECURITY_TOKEN));
        RequestAuthorization second = new RequestAuthorization(ACCESS_KEY, "us-east-1", KEY_PATH, ImmutableSet.of("host"), SIGNATURE, expiry, Optional.of(SECURITY_TOKEN));

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
        assertThat(first.toString()).contains("expiry=Optional[2026-01-01T00:00:00Z]");
    }

    private static RequestAuthorization authorization(String accessKey, String signature, Optional<String> securityToken)
    {
        return new RequestAuthorization(accessKey, "us-east-1", KEY_PATH, ImmutableSet.of("host", "x-amz-date"), signature, Optional.empty(), securityToken);
    }

    private static String tokenPart(String value)
    {
        return value.substring(value.indexOf("securityToken="));
    }
}
