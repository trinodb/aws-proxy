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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHttpCredentialsProviderConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "credentials-provider.http.endpoint", "http://usersvc:9000/api/v1/users",
                "credentials-provider.http.headers", "x-api-key: xyz123, Content-Type: application/json");
        HttpCredentialsProviderConfig expected = new HttpCredentialsProviderConfig()
                .setEndpoint("http://usersvc:9000/api/v1/users")
                .setHttpHeaders("x-api-key: xyz123, Content-Type: application/json");
        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidHttpHeaderVariation1()
    {
        HttpCredentialsProviderConfig config = new HttpCredentialsProviderConfig()
                .setEndpoint("http://usersvc:9000/api/v1/users")
                .setHttpHeaders("x-api-key: Authorization: xyz123");
        Map<String, String> httpHeaders = config.getHttpHeaders();
        assertThat(httpHeaders.get("x-api-key")).isEqualTo("Authorization: xyz123");
    }

    @Test
    public void testValidHttpHeaderVariation2()
    {
        HttpCredentialsProviderConfig config = new HttpCredentialsProviderConfig()
                .setEndpoint("http://usersvc:9000/api/v1/users")
                .setHttpHeaders("x-api-key: xyz,,123, Authorization: key,,,,123");
        Map<String, String> httpHeaders = config.getHttpHeaders();
        assertThat(httpHeaders.get("x-api-key")).isEqualTo("xyz,123");
        assertThat(httpHeaders.get("Authorization")).isEqualTo("key,,123");
    }

    @Test
    public void testIncorrectHttpHeader1()
    {
        assertThrows(IllegalArgumentException.class, () -> new HttpCredentialsProviderConfig()
                .setEndpoint("http://usersvc:9000/api/v1/users")
                .setHttpHeaders("malformed-header"));
    }

    @Test
    public void testIncorrectHttpHeader2()
    {
        assertThrows(IllegalArgumentException.class, () -> new HttpCredentialsProviderConfig()
                .setEndpoint("http://usersvc:9000/api/v1/users")
                .setHttpHeaders("x-api-key: xyz,,,123, Authorization: key123"));
    }
}
