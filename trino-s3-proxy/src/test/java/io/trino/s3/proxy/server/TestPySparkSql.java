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
package io.trino.s3.proxy.server;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.s3.proxy.server.testing.containers.PySparkContainer;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithAllContainers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static io.trino.s3.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.s3.proxy.server.testing.containers.DockerAttachUtil.executeCommand;
import static java.util.Objects.requireNonNull;
import static org.awaitility.Awaitility.await;

@TrinoS3ProxyTest(filters = WithAllContainers.class)
public class TestPySparkSql
{
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
        // create the test bucket
        s3Client.createBucket(r -> r.bucket("test"));

        // upload a CSV file as a potential table
        s3Client.putObject(r -> r.bucket("test").key("table/file.csv"), Path.of(Resources.getResource("test.csv").toURI()));

        // create the database
        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"create database db\")"));

        // create the DB
        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), """
                spark.sql(""\"
                  CREATE TABLE IF NOT EXISTS db.people(name STRING, age INT)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                  LOCATION 's3a://test/table/'
                  TBLPROPERTIES ("s3select.format" = "csv");
                  ""\")
                """));

        try (InputStream inputStream = executeCommand(pySparkContainer.containerId(), "spark.sql(\"select * from db.people\").show()")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            await().until(() -> {
                Set<String> lines = new HashSet<>();
                while (!Thread.currentThread().isInterrupted()) {
                    lines.add(reader.readLine());

                    if (lines.contains("|Person Parson| 42|") && lines.contains("|    John Galt| 28|")) {
                        break;
                    }
                }
                return true;
            });
        }
    }
}
