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
import org.junit.jupiter.api.Test;

import static io.trino.s3.proxy.server.credentials.ParsedAuthorization.Credential;
import static io.trino.s3.proxy.server.credentials.ParsedAuthorization.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParsedAuthorization
{
    @Test
    public void testValues()
    {
        assertThat(parse(null)).isEqualTo(new ParsedAuthorization("", new Credential("", ""), ImmutableSet.of(), ""));
        assertThat(parse(null).isValid()).isFalse();

        ParsedAuthorization parsedAuthorization = parse("AWS4-HMAC-SHA256 Credential=THIS_IS_AN_ACCESS_KEY/20240608/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date, Signature=c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd");
        assertThat(parsedAuthorization.isValid()).isTrue();
        assertThat(parsedAuthorization.credential()).isEqualTo(new Credential("THIS_IS_AN_ACCESS_KEY", "us-east-1"));
        assertThat(parsedAuthorization.signedHeaders()).containsExactly("amz-sdk-invocation-id", "amz-sdk-request", "amz-sdk-retry", "content-type", "host", "user-agent", "x-amz-content-sha256", "x-amz-date");
        assertThat(parsedAuthorization.signature()).isEqualTo("c23adc773b858c0bf6fa6885a047781606ab5dd114116136bd5a388d45ede8cd");

        parsedAuthorization = parse("");
        assertThat(parsedAuthorization.isValid()).isFalse();
        assertThat(parsedAuthorization.signedHeaders()).isEmpty();
        assertThat(parsedAuthorization.signature()).isEmpty();

        parsedAuthorization = parse("x=y,,,,,,SignedHeaders=");
        assertThat(parsedAuthorization.credential()).isEqualTo(new Credential("", ""));
        assertThat(parsedAuthorization.signedHeaders()).isEmpty();
        assertThat(parsedAuthorization.signature()).isEmpty();

        parsedAuthorization = parse("x=y,,,,,,SignedHeaders=b;b;b;b;c,,,,,,,,,");
        assertThat(parsedAuthorization.credential()).isEqualTo(new Credential("", ""));
        assertThat(parsedAuthorization.signedHeaders()).containsExactly("b", "c");
        assertThat(parsedAuthorization.signature()).isEmpty();

        parsedAuthorization = parse("AWS4-HMAC-SHA256 Credential=x/y/z,SignedHeaders=1,Signature=foo");
        assertThat(parsedAuthorization.isValid()).isTrue();
        assertThat(parsedAuthorization.credential()).isEqualTo(new Credential("x", "z"));
        assertThat(parsedAuthorization.signedHeaders()).containsExactly("1");
        assertThat(parsedAuthorization.signature()).isEqualTo("foo");
    }
}
