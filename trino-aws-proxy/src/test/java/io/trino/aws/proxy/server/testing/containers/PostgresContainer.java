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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class PostgresContainer
{
    private static final Logger log = Logger.get(PostgresContainer.class);

    private static final String IMAGE_NAME = "postgres";
    private static final String IMAGE_TAG = "alpine3.20";

    private final PostgreSQLContainer<?> container;

    public PostgresContainer()
    {
        container = new PostgreSQLContainer<>(DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG));
        container.start();

        log.info("PostgreSQL container started on port: " + container.getFirstMappedPort());
    }

    public String jdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public String user()
    {
        return container.getUsername();
    }

    public String password()
    {
        return container.getPassword();
    }

    public int port()
    {
        return container.getFirstMappedPort();
    }

    @PreDestroy
    public void shutdown()
    {
        container.close();
    }
}
