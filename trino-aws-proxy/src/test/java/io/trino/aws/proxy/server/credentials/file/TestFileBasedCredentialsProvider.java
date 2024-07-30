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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.ObjectMapperProvider;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.Identity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFileBasedCredentialsProvider
{
    private static FileBasedCredentialsProvider credentialsProvider;

    @BeforeAll
    public static void setUpClass()
    {
        File configFile = new File(Resources.getResource("credentials.json").getFile());
        FileBasedCredentialsProviderConfig config = new FileBasedCredentialsProviderConfig().setCredentialsFile(configFile);
        ObjectMapper objectMapper = new ObjectMapperProvider()
                .withModules(ImmutableSet.of(new SimpleModule().addAbstractTypeMapping(Identity.class, TestingIdentity.class))).get();
        credentialsProvider = new FileBasedCredentialsProvider(config, objectMapper);
    }

    @Test
    public void testValidCredentials()
    {
        Credential emulated = new Credential("test-emulated-access-key", "test-emulated-secret");
        Credential remote = new Credential("test-remote-access-key", "test-remote-secret");
        Credentials expected = new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.of(new TestingIdentity("test-username")));
        Optional<Credentials> actual = credentialsProvider.credentials("test-emulated-access-key", Optional.empty());
        assertThat(actual).contains(expected);
    }

    @Test
    public void testInvalidCredentials()
    {
        Optional<Credentials> actual = credentialsProvider.credentials("non-existent-key", Optional.empty());
        assertThat(actual).isEmpty();
    }
}
