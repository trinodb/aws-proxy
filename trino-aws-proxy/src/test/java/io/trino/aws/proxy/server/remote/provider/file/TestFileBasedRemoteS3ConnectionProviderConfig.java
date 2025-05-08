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
package io.trino.aws.proxy.server.remote.provider.file;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFileBasedRemoteS3ConnectionProviderConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("remote-s3-connection-provider.connections-file-path", "/dev/null")
                .buildOrThrow();
        FileBasedRemoteS3ConnectionProviderConfig expected = new FileBasedRemoteS3ConnectionProviderConfig()
                .setConnectionsFile(new File("/dev/null"));
        assertFullMapping(properties, expected);
    }

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileBasedRemoteS3ConnectionProviderConfig.class)
                .setConnectionsFile(null));
    }
}
