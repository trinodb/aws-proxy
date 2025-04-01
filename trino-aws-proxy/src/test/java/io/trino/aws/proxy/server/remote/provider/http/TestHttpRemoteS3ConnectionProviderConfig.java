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
package io.trino.aws.proxy.server.remote.provider.http;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHttpRemoteS3ConnectionProviderConfig
{
    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("remote-s3-connection-provider.http.endpoint", "http://localhost:8080")
                .put("remote-s3-connection-provider.http.request-fields", "bucket,key,emulated-access-key")
                .put("remote-s3-connection-provider.http.cache-size", "100")
                .put("remote-s3-connection-provider.http.cache-ttl", "10s")
                .buildOrThrow();
        HttpRemoteS3ConnectionProviderConfig expected = new HttpRemoteS3ConnectionProviderConfig()
                .setEndpoint(new URI("http://localhost:8080"))
                .setRequestFields(List.of(RequestQuery.BUCKET, RequestQuery.KEY, RequestQuery.EMULATED_ACCESS_KEY))
                .setCacheSize(100)
                .setCacheTtl(Duration.valueOf("10s"));
        assertFullMapping(properties, expected);
    }

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HttpRemoteS3ConnectionProviderConfig.class)
                .setEndpoint(null)
                .setRequestFields(EnumSet.allOf(RequestQuery.class).stream().collect(toImmutableList()))
                .setCacheSize(0)
                .setCacheTtl(Duration.valueOf("1s")));
    }
}
