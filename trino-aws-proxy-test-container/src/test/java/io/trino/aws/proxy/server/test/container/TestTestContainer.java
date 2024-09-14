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
package io.trino.aws.proxy.server.test.container;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;

import static io.trino.aws.proxy.server.test.container.TrinoAwsProxyServerTestContainer.projectVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public class TestTestContainer
{
    @Test
    public void testBasicContainerStart()
            throws Exception
    {
        try (TrinoAwsProxyServerTestContainer testContainer = new TrinoAwsProxyServerTestContainer()) {
            testContainer.start();

            URI uri = URI.create("http://localhost:%s/".formatted(testContainer.getFirstMappedPort()));
            HttpURLConnection urlConnection = (HttpURLConnection) uri.toURL().openConnection();
            int responseCode = urlConnection.getResponseCode();
            assertThat(responseCode).isEqualTo(404);
        }
    }

    @Test
    // note this tests relies on this module's maven POM to copy the main module's test-jar into
    // this module's target directory. The main module's test-jar has MockPlugin.
    public void testPlugin()
    {
        String jarName = "trino-aws-proxy-%s-tests.jar".formatted(projectVersion());
        File targetDirectory = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getFile()).getParentFile();
        File jarFile = new File(targetDirectory, jarName);

        try (TrinoAwsProxyServerTestContainer testContainer = new TrinoAwsProxyServerTestContainer()) {
            testContainer.withFileSystemBind(jarFile.getAbsolutePath(), "/opt/mock.jar", READ_ONLY)
                    .withAdditionalLib("/opt/mock.jar")
                    .start();
            assertThat(testContainer.getLogs()).contains("Loading plugin: MockPlugin");
        }
    }
}
