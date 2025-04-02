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

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.testing.TestingUtil;
import io.trino.aws.proxy.spi.credentials.Credential;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.asHostUrl;
import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.exposeHostPort;

public class MetastoreContainer
{
    private static final Logger log = Logger.get(MetastoreContainer.class);

    private static final String IMAGE_NAME = "apache/hive";
    private static final String IMAGE_TAG = "4.0.0";

    private final GenericContainer<?> container;

    @SuppressWarnings("resource")
    @Inject
    public MetastoreContainer(PostgresContainer postgresContainer, TestingHttpServer httpServer, @S3Container.ForS3Container Credential remoteCredential, S3Container s3Container)
            throws IOException
    {
        String s3Endpoint = asHostUrl(s3Container.endpoint().toString());

        File postgresJar = TestingUtil.findTestJar("postgres");
        File hadoopJar = TestingUtil.findTestJar("hadoop");
        File awsSdkJar = TestingUtil.findTestJar("aws");

        String hiveSiteXml = Resources.toString(Resources.getResource("hive-site.xml"), StandardCharsets.UTF_8)
                .replace("$ENDPOINT$", s3Endpoint)
                .replace("$ACCESS_KEY$", remoteCredential.accessKey())
                .replace("$SECRET_KEY$", remoteCredential.secretKey());

        // need to disable SSL to postgres otherwise HMS throws an exception and quits
        String serviceOpts = "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=%s -Djavax.jdo.option.ConnectionUserName=%s -Djavax.jdo.option.ConnectionPassword=%s"
                .formatted(asHostUrl(postgresContainer.jdbcUrl()) + "&sslmode=disable", postgresContainer.user(), postgresContainer.password());

        container = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG))
                .withEnv("SERVICE_NAME", "metastore")
                .withEnv("DB_DRIVER", "postgres")
                .withEnv("SERVICE_OPTS", serviceOpts)
                .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS))
                .withExposedPorts(9083)
                .withFileSystemBind(postgresJar.getAbsolutePath(), "/opt/hive/lib/postgres.jar", BindMode.READ_ONLY)
                .withFileSystemBind(hadoopJar.getAbsolutePath(), "/opt/hive/lib/hadoop.jar", BindMode.READ_ONLY)
                .withFileSystemBind(awsSdkJar.getAbsolutePath(), "/opt/hive/lib/aws.jar", BindMode.READ_ONLY)
                .withCopyToContainer(Transferable.of(hiveSiteXml), "/opt/hive/conf/hive-site.xml")
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Starting Hive Metastore Server.*"));

        exposeHostPort(httpServer.getPort());
        exposeHostPort(s3Container.containerHost().getPort());
        exposeHostPort(postgresContainer.port());

        container.start();

        log.info("Hive Metastore Server started on port: " + container.getFirstMappedPort());
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
