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

import com.google.common.net.HostAndPort;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static io.trino.aws.proxy.server.testing.TestingUtil.LOCALHOST_DOMAIN;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class S3Container
        implements Provider<S3Client>
{
    private static final Logger log = Logger.get(S3Container.class);

    public static final String POLICY_NAME = "managedPolicy";

    private static final String IMAGE_NAME = "minio/minio";
    private static final String IMAGE_TAG = "RELEASE.2023-09-04T19-57-37Z";

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

    private static final String POLICY = """
            {
               "Version": "2012-10-17",
               "Statement": [
                  {
                     "Effect": "Allow",
                     "Action": [
                        "s3:*"
                     ],
                     "Resource": [
                        "arn:aws:s3:::*"
                     ]
                  }
               ]
            }
            """;

    private final MinIOContainer container;
    private final S3Client storageClient;
    private final List<String> initialBuckets;
    private final Credential credential;
    private final Credential policyUserCredential;

    private volatile Credentials sessionCredentials;

    @Override
    public S3Client get()
    {
        return storageClient;
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForS3Container {}

    @Inject
    public S3Container(@ForS3Container List<String> initialBuckets, @ForTesting Credentials credentials)
    {
        this.initialBuckets = requireNonNull(initialBuckets, "initialBuckets is null");
        this.credential = requireNonNull(credentials, "credentials is null").requiredRemoteCredential();

        Transferable config = Transferable.of(CONFIG_TEMPLATE.formatted(credential.accessKey(), credential.secretKey()));
        Transferable policyFile = Transferable.of(POLICY);

        container = new MinIOContainer(DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG))
                .withUserName(credential.accessKey())
                .withPassword(credential.secretKey())
                // setting this allows us to shell into the container and run "mc" commands
                .withCopyToContainer(config, "/root/.mc/config.json")
                .withCopyToContainer(policyFile, "/root/policy.json");

        container.withEnv("MINIO_DOMAIN", LOCALHOST_DOMAIN);
        container.start();

        log.info("S3 container started on port: %s", container.getFirstMappedPort());

        storageClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(container.getS3URL()))
                .forcePathStyle(true)
                .credentialsProvider(() -> AwsBasicCredentials.create(credential.accessKey(), credential.secretKey()))
                .build();

        policyUserCredential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    public HostAndPort containerHost()
    {
        return HostAndPort.fromParts(container.getHost(), container.getFirstMappedPort());
    }

    public Credential policyUserCredential()
    {
        return policyUserCredential;
    }

    @PostConstruct
    public void setUp()
    {
        initialBuckets.forEach(bucket -> storageClient.createBucket(request -> request.bucket(bucket)));

        // the Minio client does not have APIs for IAM or STS
        execInContainer("Could not create user in container", "mc", "admin", "user", "add", "local", policyUserCredential.accessKey(), policyUserCredential.secretKey());
        execInContainer("Could not create policy in container", "mc", "admin", "policy", "create", "local", POLICY_NAME, "/root/policy.json");
        execInContainer("Could not attach policy in container", "mc", "admin", "policy", "attach", "local", POLICY_NAME, "--user", policyUserCredential.accessKey());
    }

    @PreDestroy
    public void shutdown()
    {
        container.stop();
    }

    private void execInContainer(String errorMessage, String... command)
    {
        Container.ExecResult execResult;
        try {
            execResult = container.execInContainer(command);
        }
        catch (Exception e) {
            throw new RuntimeException(errorMessage, e);
        }
        if (execResult.getExitCode() != 0) {
            throw new RuntimeException(errorMessage + "\n" + execResult.getStdout() + "\n" + execResult.getStderr());
        }
    }
}
