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
package io.trino.aws.proxy.server;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithAllContainers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;

import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.inputToContainerStdin;
import static java.util.Objects.requireNonNull;

@TrinoAwsProxyTest(filters = WithAllContainers.class)
public class TestPySparkSql
{
    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "people";

    private final S3Client s3Client;
    private final PySparkContainer pySparkContainer;

    @Inject
    public TestPySparkSql(S3Client s3Client, PySparkContainer pySparkContainer)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.pySparkContainer = requireNonNull(pySparkContainer, "pySparkContainer is null");
    }

    @Test
    public void testSql()
            throws Exception
    {
        createDatabaseAndTable(s3Client, pySparkContainer);

        clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(), "spark.sql(\"select * from %s.%s\").show()".formatted(DATABASE_NAME, TABLE_NAME)), line -> line.equals("|    John Galt| 28|"));
    }

    public static void createDatabaseAndTable(S3Client s3Client, PySparkContainer container)
            throws Exception
    {
        // create the test bucket
        s3Client.createBucket(r -> r.bucket("test"));

        // upload a CSV file as a potential table
        s3Client.putObject(r -> r.bucket("test").key("table/file.csv"), Path.of(Resources.getResource("test.csv").toURI()));

        // create the database
        clearInputStreamAndClose(inputToContainerStdin(container.containerId(), "spark.sql(\"create database %s\")".formatted(DATABASE_NAME)), line -> line.equals("DataFrame[]"));

        // create the DB
        clearInputStreamAndClose(inputToContainerStdin(container.containerId(), """
                spark.sql(""\"
                  CREATE TABLE IF NOT EXISTS %s.%s(name STRING, age INT)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                  LOCATION 's3a://test/table/'
                  TBLPROPERTIES ("s3select.format" = "csv");
                  ""\")
                """.formatted(DATABASE_NAME, TABLE_NAME)), line -> line.equals("DataFrame[]"));
    }
}
