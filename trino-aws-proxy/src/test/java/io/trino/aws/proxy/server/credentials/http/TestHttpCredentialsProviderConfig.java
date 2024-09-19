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
package io.trino.aws.proxy.server.credentials.http;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHttpCredentialsProviderConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("credentials-provider.http.endpoint", "http://usersvc:9000/api/v1/users")
                .put("credentials-provider.http.headers", "x-api-key: xyz123, Content-Type: application/json")
                .put("credentials-provider.http.cache-size", "123")
                .put("credentials-provider.http.cache-ttl", "2m")
                .buildOrThrow();
        HttpCredentialsProviderConfig expected = new HttpCredentialsProviderConfig()
                .setEndpoint(URI.create("http://usersvc:9000/api/v1/users"))
                .setHttpHeaders("x-api-key: xyz123, Content-Type: application/json")
                .setCacheSize(123)
                .setCacheTtl(new Duration(2, TimeUnit.MINUTES));
        assertFullMapping(properties, expected);
    }

    @Test
    public void testConfigDefaults()
    {
        assertRecordedDefaults(recordDefaults(HttpCredentialsProviderConfig.class)
                .setEndpoint(null)
                .setHttpHeaders("")
                .setCacheSize(0)
                .setCacheTtl(Duration.ZERO));
    }

    @Test
    public void testValidHttpHeaderVariation1()
    {
        HttpCredentialsProviderConfig config = new HttpCredentialsProviderConfig()
                .setEndpoint(URI.create("http://usersvc:9000/api/v1/users"))
                .setHttpHeaders("x-api-key: Authorization: xyz123");
        Map<String, String> httpHeaders = config.getHttpHeaders();
        assertThat(httpHeaders.get("x-api-key")).isEqualTo("Authorization: xyz123");
    }

    @Test
    public void testValidHttpHeaderVariation2()
    {
        HttpCredentialsProviderConfig config = new HttpCredentialsProviderConfig()
                .setEndpoint(URI.create("http://usersvc:9000/api/v1/users"))
                .setHttpHeaders("x-api-key: xyz,,123, Authorization: key,,,,123");
        Map<String, String> httpHeaders = config.getHttpHeaders();
        assertThat(httpHeaders.get("x-api-key")).isEqualTo("xyz,123");
        assertThat(httpHeaders.get("Authorization")).isEqualTo("key,,123");
    }

    @Test
    public void testIncorrectHttpHeader1()
    {
        assertThrows(IllegalArgumentException.class, () -> new HttpCredentialsProviderConfig()
                .setEndpoint(URI.create("http://usersvc:9000/api/v1/users"))
                .setHttpHeaders("malformed-header"));
    }

    @Test
    public void testIncorrectHttpHeader2()
    {
        assertThrows(IllegalArgumentException.class, () -> new HttpCredentialsProviderConfig()
                .setEndpoint(URI.create("http://usersvc:9000/api/v1/users"))
                .setHttpHeaders("x-api-key: xyz,,,123, Authorization: key123"));
    }
}
