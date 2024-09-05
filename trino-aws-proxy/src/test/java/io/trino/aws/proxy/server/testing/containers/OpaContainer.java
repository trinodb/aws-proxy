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
package io.trino.aws.proxy.server.testing.containers;

import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class OpaContainer
{
    private static final Logger log = Logger.get(OpaContainer.class);

    private static final String IMAGE_NAME = "openpolicyagent/opa";
    private static final String IMAGE_TAG = "0.68.0-dev-static-debug";

    private final GenericContainer<?> container;

    @SuppressWarnings("resource")
    public OpaContainer()
    {
        DockerImageName dockerImageName = DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG);
        container = new GenericContainer<>(dockerImageName)
                .withCreateContainerCmdModifier(modifier -> modifier.withTty(true))
                .withCommand("run", "--server")
                .withExposedPorts(8181);
        container.start();
    }

    public int getPort()
    {
        return container.getFirstMappedPort();
    }

    public String getHost()
    {
        return container.getHost();
    }

    @PreDestroy
    public void shutdown()
    {
        container.close();
    }
}
