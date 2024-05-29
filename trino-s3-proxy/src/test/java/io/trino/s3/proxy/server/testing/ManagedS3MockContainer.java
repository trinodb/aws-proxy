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

import com.google.common.base.Splitter;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.Credentials.Credential;
import io.trino.s3.proxy.server.testing.TestingConstants.ForTestingCredentials;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.images.builder.Transferable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class ManagedS3MockContainer
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
    private final String initialBuckets;
    private final Credential credential;

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
    public ManagedS3MockContainer(@ForS3MockContainer String initialBuckets, @ForTestingCredentials Credentials credentials)
    {
        this.initialBuckets = requireNonNull(initialBuckets, "initialBuckets is null");
        this.credential = requireNonNull(credentials, "credentials is null").real();

        Transferable transferable = Transferable.of(CONFIG_TEMPLATE.formatted(credential.accessKey(), credential.secretKey()));
        container = new MinIOContainer(MINIO_IMAGE_NAME + ":" + MINIO_IMAGE_TAG)
                .withUserName(credential.accessKey())
                .withPassword(credential.secretKey())
                // setting this allows us to shell into the container and run "mc" commands
                .withCopyToContainer(transferable, "/root/.mc/config.json");

        container.start();
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

        MinioClient client = MinioClient.builder()
                .credentials(credential.accessKey(), credential.secretKey())
                .endpoint(container.getS3URL())
                .build();

        Splitter.on(',').trimResults().splitToStream(initialBuckets).forEach(bucket -> {
            try {
                client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
            }
            catch (Exception e) {
                throw new RuntimeException("Could not create bucket: " + bucket, e);
            }
        });
    }

    @PreDestroy
    public void shutdown()
    {
        container.stop();
    }
}
