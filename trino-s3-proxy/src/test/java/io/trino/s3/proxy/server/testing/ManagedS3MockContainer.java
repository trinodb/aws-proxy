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
package io.trino.s3.proxy.server.testing;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.testing.TestingConstants.ForTesting;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.images.builder.Transferable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;

import static io.trino.s3.proxy.server.testing.TestingConstants.LOCALHOST_DOMAIN;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class ManagedS3MockContainer
        implements Provider<S3Client>
{
    private static final String MINIO_IMAGE_NAME = "minio/minio";
    private static final String MINIO_IMAGE_TAG = "RELEASE.2023-09-04T19-57-37Z";

    private static final String CONFIG_TEMPLATE = """
            {
                "version": "10",
                "aliases": {
                    "local": {
                        "url": "http://localhost:9000",
                        "accessKey": "%s",
                        "secretKey": "%s",
                        "api": "S3v4",
                        "path": "auto"
                    }
                }
            }
            """;

    private final MinIOContainer container;
    private final S3Client storageClient;
    private final List<String> initialBuckets;
    private final Credential credential;

    @Override
    public S3Client get()
    {
        return storageClient;
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForS3MockContainer {}

    public record ContainerHost(String host, Integer port)
    {
        public ContainerHost
        {
            requireNonNull(host, "host is null");
            requireNonNull(port, "port is null");
        }
    }

    @Inject
    public ManagedS3MockContainer(@ForS3MockContainer List<String> initialBuckets, @ForTesting Credentials credentials)
    {
        this.initialBuckets = requireNonNull(initialBuckets, "initialBuckets is null");
        this.credential = requireNonNull(credentials, "credentials is null").requiredRealCredential();

        Transferable transferable = Transferable.of(CONFIG_TEMPLATE.formatted(credential.accessKey(), credential.secretKey()));
        container = new MinIOContainer(MINIO_IMAGE_NAME + ":" + MINIO_IMAGE_TAG)
                .withUserName(credential.accessKey())
                .withPassword(credential.secretKey())
                // setting this allows us to shell into the container and run "mc" commands
                .withCopyToContainer(transferable, "/root/.mc/config.json");

        container.withEnv("MINIO_DOMAIN", LOCALHOST_DOMAIN);
        container.start();
        storageClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(container.getS3URL()))
                .forcePathStyle(true)
                .credentialsProvider(() -> AwsBasicCredentials.create(credential.accessKey(), credential.secretKey()))
                .build();
    }

    public ContainerHost getContainerHost()
    {
        return new ContainerHost(container.getHost(), container.getFirstMappedPort());
    }

    @PostConstruct
    public void setUp()
    {
        if (initialBuckets.isEmpty()) {
            return;
        }

        initialBuckets.forEach(bucket -> storageClient.createBucket(request -> request.bucket(bucket)));
    }

    @PreDestroy
    public void shutdown()
    {
        container.stop();
    }
}
