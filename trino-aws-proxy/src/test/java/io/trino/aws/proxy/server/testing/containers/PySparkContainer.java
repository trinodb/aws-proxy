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

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;

import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.inputToContainerStdin;
import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.asHostUrl;
import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.exposeHostPort;

public class PySparkContainer
{
    private static final Logger log = Logger.get(PySparkContainer.class);

    private static final String IMAGE_NAME = "spark";
    private static final String IMAGE_TAG = "3.5.1-scala2.12-java17-python3-ubuntu";

    private final GenericContainer<?> container;

    @SuppressWarnings("resource")
    @Inject
    public PySparkContainer(MetastoreContainer metastoreContainer, TestingHttpServer httpServer, @ForTesting Credentials testingCredentials, TrinoS3ProxyConfig trinoS3ProxyConfig)
            throws IOException
    {
        File hadoopJar = TestingUtil.findTestJar("hadoop");
        File awsSdkJar = TestingUtil.findTestJar("aws");

        container = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG))
                .withFileSystemBind(hadoopJar.getAbsolutePath(), "/opt/spark/jars/hadoop.jar", BindMode.READ_ONLY)
                .withFileSystemBind(awsSdkJar.getAbsolutePath(), "/opt/spark/jars/aws.jar", BindMode.READ_ONLY)
                .withCreateContainerCmdModifier(modifier -> modifier.withTty(true).withStdinOpen(true).withAttachStdin(true).withAttachStdout(true).withAttachStderr(true))
                .withCommand("/opt/spark/bin/pyspark");

        exposeHostPort(httpServer.getPort());
        exposeHostPort(metastoreContainer.port());

        container.start();

        String metastoreEndpoint = asHostUrl("localhost:" + metastoreContainer.port());
        String s3Endpoint = asHostUrl(httpServer.getBaseUrl().resolve(trinoS3ProxyConfig.getS3Path()).toString());

        clearInputStreamAndClose(inputToContainerStdin(container.getContainerId(), "spark.stop()"));
        clearInputStreamAndClose(inputToContainerStdin(container.getContainerId(), """
                spark = SparkSession\\
                    .builder\\
                    .appName("testing")\\
                    .config("hive.metastore.uris", "thrift://%s")\\
                    .enableHiveSupport()\\
                    .config("spark.hadoop.fs.s3a.endpoint", "%s")\\
                    .config("spark.hadoop.fs.s3a.access.key", "%s")\\
                    .config("spark.hadoop.fs.s3a.secret.key", "%s")\\
                    .config("spark.hadoop.fs.s3a.path.style.access", True)\\
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\\
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\\
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False)\\
                    .getOrCreate()
                """.formatted(metastoreEndpoint, s3Endpoint, testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey())));

        log.info("PySpark container started");
    }

    public String containerId()
    {
        return container.getContainerId();
    }

    @PreDestroy
    public void shutdown()
    {
        container.close();
    }
}
