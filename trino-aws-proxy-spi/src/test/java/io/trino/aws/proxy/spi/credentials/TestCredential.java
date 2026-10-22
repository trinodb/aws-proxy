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
package io.trino.aws.proxy.spi.credentials;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCredential
{
    private static final String ACCESS_KEY = "AKIAEXAMPLEACCESSKEY";
    private static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static final String SESSION = "FwoGZXIvYXdzEXAMPLESESSIONTOKENVALUE1234567890";

    @Test
    public void testToStringRedactsSecrets()
    {
        String value = new Credential(ACCESS_KEY, SECRET_KEY, Optional.of(SESSION)).toString();

        assertThat(value)
                .startsWith("Credential[accessKey=****SKEY, secretKey=<redacted,len=%s>, session=<redacted,len=%s,sha256=".formatted(SECRET_KEY.length(), SESSION.length()))
                .doesNotContain(ACCESS_KEY)
                .doesNotContain(SECRET_KEY)
                .doesNotContain(SECRET_KEY.substring(0, 4))
                .doesNotContain(SECRET_KEY.substring(SECRET_KEY.length() - 4))
                .doesNotContain(SESSION)
                .doesNotContain(SESSION.substring(0, 8));
    }

    @Test
    public void testEmptySession()
    {
        assertThat(new Credential(ACCESS_KEY, SECRET_KEY).toString())
                .endsWith("session=Optional.empty]");
    }

    @Test
    public void testShortValues()
    {
        assertThat(new Credential("", "", Optional.of("")).toString())
                .isEqualTo("Credential[accessKey=****, secretKey=<redacted,len=0>, session=<redacted,len=0,sha256=e3b0c442>]");
        assertThat(new Credential("AKIA", "sk", Optional.of("tok")).toString())
                .doesNotContain("AKIA")
                .doesNotContain("sk>")
                .doesNotContain("tok>");
    }

    @Test
    public void testSameSessionRendersSameHash()
    {
        String first = new Credential(ACCESS_KEY, SECRET_KEY, Optional.of(SESSION)).toString();
        String second = new Credential("AKIAOTHERACCESSKEY99", "other-secret", Optional.of(SESSION)).toString();

        assertThat(first.substring(first.indexOf("session="))).isEqualTo(second.substring(second.indexOf("session=")));
    }

    @Test
    public void testEqualityUnchanged()
    {
        Credential first = new Credential(ACCESS_KEY, SECRET_KEY, Optional.of(SESSION));
        Credential second = new Credential(ACCESS_KEY, SECRET_KEY, Optional.of(SESSION));

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
        assertThat(first).isNotEqualTo(new Credential(ACCESS_KEY, SECRET_KEY));
    }
}
