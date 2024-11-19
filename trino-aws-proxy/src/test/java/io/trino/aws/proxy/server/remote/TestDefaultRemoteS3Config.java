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
package io.trino.aws.proxy.server.remote;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDefaultRemoteS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DefaultRemoteS3Config.class)
                .setDomain("amazonaws.com").setHttps(true).setPort(null)
                .setVirtualHostStyle(true)
                .setHostnameTemplate("${bucket}.s3.${region}.${domain}"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "remoteS3.https", "false",
                "remoteS3.domain", "testS3Domain.com",
                "remoteS3.port", "80",
                "remoteS3.virtual-host-style", "false",
                "remoteS3.hostname.template", "s3.${region}.${domain}");

        DefaultRemoteS3Config expected = new DefaultRemoteS3Config().setHttps(false).setDomain("testS3Domain.com").setPort(80).setVirtualHostStyle(false)
                .setHostnameTemplate("s3.${region}.${domain}");
        assertFullMapping(properties, expected);
    }
}
