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
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.File;

import static io.trino.aws.proxy.server.testing.TestingUtil.findTestJar;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.inputToContainerStdin;
import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.asHostUrl;
import static io.trino.aws.proxy.server.testing.containers.TestContainerUtil.exposeHostPort;

public abstract class PySparkContainer
{
    private static final Logger log = Logger.get(PySparkContainer.class);

    private static final String V3_IMAGE_NAME = "spark";
    private static final String V3_IMAGE_TAG = "3.5.1-scala2.12-java17-python3-ubuntu";

    private static final String V4_IMAGE_NAME = "apache/spark";
    private static final String V4_IMAGE_TAG = "4.0.0-preview1-scala2.13-java17-python3-ubuntu";

    private final GenericContainer<?> container;

    public static class PySparkV3Container
            extends PySparkContainer
    {
        @Inject
        public PySparkV3Container(
                MetastoreContainer metastoreContainer,
                S3Container s3Container,
                TestingHttpServer httpServer,
                @ForTesting IdentityCredential testingCredentials,
                TrinoAwsProxyConfig trinoS3ProxyConfig)
        {
            super(metastoreContainer, s3Container, httpServer, testingCredentials, trinoS3ProxyConfig, Version.VERSION_3);
        }
    }

    public static class PySparkV4Container
            extends PySparkContainer
    {
        @Inject
        public PySparkV4Container(
                MetastoreContainer metastoreContainer,
                S3Container s3Container,
                TestingHttpServer httpServer,
                @ForTesting IdentityCredential testingCredentials,
                TrinoAwsProxyConfig trinoS3ProxyConfig)
        {
            super(metastoreContainer, s3Container, httpServer, testingCredentials, trinoS3ProxyConfig, Version.VERSION_4);
        }
    }

    private enum Version
    {
        VERSION_3,
        VERSION_4,
    }

    @SuppressWarnings("resource")
    private PySparkContainer(
            MetastoreContainer metastoreContainer,
            S3Container s3Container,
            TestingHttpServer httpServer,
            IdentityCredential testingCredentials,
            TrinoAwsProxyConfig trinoS3ProxyConfig,
            Version version)
    {
        File trinoClientDirectory = switch (version) {
            case VERSION_3 -> findTestJar("trino-aws-proxy-spark3");
            case VERSION_4 -> findTestJar("trino-aws-proxy-spark4");
        };

        File awsSdkJar = switch (version) {
            case VERSION_3 -> findTestJar("aws");
            case VERSION_4 -> findTestJar("bundle");
        };

        File hadoopJar = switch (version) {
            case VERSION_3 -> findTestJar("hadoop-aws-3.3.4");
            case VERSION_4 -> findTestJar("hadoop-aws-3.4.0");
        };

        DockerImageName dockerImageName = switch (version) {
            case VERSION_3 -> DockerImageName.parse(V3_IMAGE_NAME).withTag(V3_IMAGE_TAG);
            case VERSION_4 -> DockerImageName.parse(V4_IMAGE_NAME).withTag(V4_IMAGE_TAG);
        };

        String clientFactoryClassName = switch (version) {
            case VERSION_3 -> "io.trino.aws.proxy.spark3.TrinoAwsProxyS3ClientFactory";
            case VERSION_4 -> "io.trino.aws.proxy.spark4.TrinoAwsProxyS4ClientFactory";
        };

        String s3Endpoint = asHostUrl(httpServer.getBaseUrl().resolve(trinoS3ProxyConfig.getS3Path()).toString());
        String metastoreEndpoint = asHostUrl("localhost:" + metastoreContainer.port());

        String sparkConfFile = """
                hive.metastore.uris                          %s
                spark.hadoop.fs.s3a.endpoint                 %s
                spark.hadoop.fs.s3a.s3.client.factory.impl   %s
                spark.hadoop.fs.s3a.access.key               %s
                spark.hadoop.fs.s3a.secret.key               %s
                spark.hadoop.fs.s3a.path.style.access        True
                spark.hadoop.fs.s3a.connection.ssl.enabled   False
                spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
                spark.hadoop.fs.s3a.impl                     org.apache.hadoop.fs.s3a.S3AFileSystem
                """.formatted(metastoreEndpoint, s3Endpoint, clientFactoryClassName, testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey());

        container = new GenericContainer<>(dockerImageName)
                .withFileSystemBind(hadoopJar.getAbsolutePath(), "/opt/spark/jars/hadoop.jar", BindMode.READ_ONLY)
                .withFileSystemBind(awsSdkJar.getAbsolutePath(), "/opt/spark/jars/aws.jar", BindMode.READ_ONLY)
                .withFileSystemBind(trinoClientDirectory.getAbsolutePath(), "/opt/spark/jars/TrinoAwsProxyClient.jar", BindMode.READ_ONLY)
                .withCopyToContainer(Transferable.of(sparkConfFile), "/opt/spark/conf/spark-defaults.conf")
                .withCreateContainerCmdModifier(modifier -> modifier.withTty(true).withStdinOpen(true).withAttachStdin(true).withAttachStdout(true).withAttachStderr(true))
                .withCommand("/opt/spark/bin/pyspark");

        exposeHostPort(s3Container.containerHost().getPort());
        exposeHostPort(httpServer.getPort());
        exposeHostPort(metastoreContainer.port());

        container.start();

        clearInputStreamAndClose(inputToContainerStdin(container.getContainerId(), "spark.stop()"));
        clearInputStreamAndClose(inputToContainerStdin(container.getContainerId(), """
                spark = SparkSession\\
                    .builder\\
                    .appName("testing")\\
                    .enableHiveSupport()\\
                    .getOrCreate()
                """));

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
