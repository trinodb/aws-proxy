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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import static io.trino.aws.proxy.server.credentials.file.FileBasedCredentialsModule.FILE_BASED_CREDENTIALS_IDENTIFIER;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.bindIdentityType;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = TestFileBasedCredentialsProvider.Filter.class)
public class TestFileBasedCredentialsProvider
{
    private final CredentialsProvider credentialsProvider;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            File configFile = new File(Resources.getResource("credentials.json").getFile());

            return builder.withoutTestingCredentialsRoleProviders()
                    .addModule(new FileBasedCredentialsModule())
                    .addModule(binder -> bindIdentityType(binder, TestingIdentity.class))
                    .withProperty("credentials-provider.type", FILE_BASED_CREDENTIALS_IDENTIFIER)
                    .withProperty("credentials-provider.credentials-file-path", configFile.getAbsolutePath());
        }
    }

    @Inject
    public TestFileBasedCredentialsProvider(CredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
    }

    @Test
    public void testValidCredentials()
    {
        Credential emulated = new Credential("test-emulated-access-key", "test-emulated-secret");
        Credential remote = new Credential("test-remote-access-key", "test-remote-secret");
        Credentials expected = new Credentials(emulated, Optional.of(remote), Optional.empty(), Optional.of(new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq")));
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
