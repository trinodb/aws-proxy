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

import java.util.Optional;

import static io.trino.aws.proxy.spi.signing.RequestAuthorization.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRequestAuthorization
{
    @Test
    public void testValues()
    {
        assertThat(parse("")).isEqualTo(new RequestAuthorization("", "", "", "", ImmutableSet.of(), "", Optional.empty()));
        assertThat(parse("").isValid()).isFalse();
        assertThat(parse("").securityToken()).isEmpty();

        RequestAuthorization requestAuthorization = parse("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240608/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date, Signature=c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd");
        assertThat(requestAuthorization.isValid()).isTrue();
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("THIS_IS_AN_ACCESS_KEY", "us-east-1");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactly("amz-sdk-invocation-id", "amz-sdk-request", "amz-sdk-retry", "content-type", "host", "user-agent", "x-amz-content-sha256", "x-amz-date");
        assertThat(requestAuthorization.signature()).isEqualTo("c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd");
        assertThat(requestAuthorization.securityToken()).isEmpty();

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

        requestAuthorization = parse("AWS4-HMAC-SHA256 Credential=x/y/z,SignedHeaders=1,Signature=foo", Optional.of("some-token"));
        assertThat(requestAuthorization.isValid()).isTrue();
        assertThat(requestAuthorization).extracting(RequestAuthorization::accessKey, RequestAuthorization::region).containsExactly("x", "z");
        assertThat(requestAuthorization.lowercaseSignedHeaders()).containsExactly("1");
        assertThat(requestAuthorization.signature()).isEqualTo("foo");
        assertThat(requestAuthorization.securityToken()).contains("some-token");
    }
}
