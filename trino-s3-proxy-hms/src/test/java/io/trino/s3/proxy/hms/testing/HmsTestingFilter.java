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
package io.trino.s3.proxy.hms.testing;

import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.s3.proxy.server.testing.containers.PostgresContainer;
import io.trino.s3.proxy.server.testing.harness.BuilderFilter;

public class HmsTestingFilter
        implements BuilderFilter
{
    @Override
    public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
    {
        PostgresContainer postgresContainer = new PostgresContainer();

        return builder.withPostgresContainer(postgresContainer)
                .withMetastoreContainer()
                .withS3Container()
                .withPySparkContainer()
                .withProperty("hms.jdbc.url", postgresContainer.jdbcUrl())
                .withProperty("hms.jdbc.username", postgresContainer.user())
                .withProperty("hms.jdbc.password", postgresContainer.password());
    }
}
