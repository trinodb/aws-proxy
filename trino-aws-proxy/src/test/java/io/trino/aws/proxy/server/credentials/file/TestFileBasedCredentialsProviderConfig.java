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
package io.trino.aws.proxy.server.credentials.file;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFileBasedCredentialsProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileBasedCredentialsProviderConfig.class)
                .setCredentialsFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        File configFile = File.createTempFile("credentials", ".json");
        Map<String, String> properties = ImmutableMap.of(
                "credentials-provider.credentials-file-path", configFile.toString());

        FileBasedCredentialsProviderConfig expected = new FileBasedCredentialsProviderConfig().setCredentialsFile(configFile);
        assertFullMapping(properties, expected);
    }
}
