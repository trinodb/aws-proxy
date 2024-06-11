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
package io.trino.s3.proxy.hms;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.s3.proxy.hms.testing.HmsTestingFilter;
import io.trino.s3.proxy.hms.testing.TestingHmsSecurityFacade;
import io.trino.s3.proxy.server.testing.containers.PySparkContainer;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacade;
import io.trino.s3.proxy.spi.security.SecurityResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.s3.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.s3.proxy.server.testing.containers.DockerAttachUtil.executeCommand;
import static java.util.Objects.requireNonNull;

@TrinoS3ProxyTest(filters = HmsTestingFilter.class)
public class TestTablesController
{
    private final TablesController tablesController;
    private final PySparkContainer pySparkContainer;
    private final S3Client s3Client;
    private final TestingHmsSecurityFacade hmsSecurityFacade;

    @Inject
    public TestTablesController(TablesController tablesController, PySparkContainer pySparkContainer, S3Client s3Client, TestingHmsSecurityFacade hmsSecurityFacade)
    {
        this.tablesController = requireNonNull(tablesController, "tablesController is null");
        this.pySparkContainer = requireNonNull(pySparkContainer, "pySparkContainer is null");
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.hmsSecurityFacade = requireNonNull(hmsSecurityFacade, "hmsSecurityFacade is null");

        tablesController.start();
    }

    @BeforeAll
    public void initializeMetastoreAndS3()
            throws URISyntaxException
    {
        s3Client.createBucket(r -> r.bucket("database"));
        s3Client.putObject(r -> r.bucket("database").key("status/statuses.csv"), Path.of(Resources.getResource("status.csv").toURI()));
        s3Client.putObject(r -> r.bucket("database").key("ids/numbers/numbers001.csv"), Path.of(Resources.getResource("numbers.csv").toURI()));
        s3Client.putObject(r -> r.bucket("database").key("ids/numbers/numbers002.csv"), Path.of(Resources.getResource("numbers2.csv").toURI()));

        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"create database db\")"), line -> line.equals("DataFrame[]"));

        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), """
                spark.sql(""\"
                  CREATE TABLE IF NOT EXISTS db.statuses(name STRING, age INT)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                  LOCATION 's3a://database/status/'
                  TBLPROPERTIES ("s3select.format" = "csv");
                  ""\")
                """), line -> line.equals("DataFrame[]"));

        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), """
                spark.sql(""\"
                  CREATE TABLE IF NOT EXISTS db.numbers(name STRING, age INT)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                  LOCATION 's3a://database/ids/numbers'
                  TBLPROPERTIES ("s3select.format" = "csv");
                  ""\")
                """), line -> line.equals("DataFrame[]"));

        tablesController.refresh();
    }

    @Test
    public void test()
    {
        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"select * from db.statuses\").show()"), line -> line.equals("|      Adam|    Apple|"));
        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"select * from db.numbers\").show()"), line -> line.equals("|1001|     1|"));

        hmsSecurityFacade.setDelegate((request, credentials, session) -> new HmsSecurityFacade()
        {
            @Override
            public SecurityResponse canQueryTable(String region, String databaseName, String tableName)
            {
                return new SecurityResponse(!tableName.equals("numbers"), Optional.empty());
            }
        });

        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"select * from db.statuses\").show()"), line -> line.equals("|      Adam|    Apple|"));
        clearInputStreamAndClose(executeCommand(pySparkContainer.containerId(), "spark.sql(\"select * from db.numbers\").show()"), line -> line.startsWith("java.nio.file.AccessDeniedException"));
    }
}
