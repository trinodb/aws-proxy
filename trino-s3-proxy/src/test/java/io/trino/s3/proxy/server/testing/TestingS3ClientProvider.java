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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import static io.trino.s3.proxy.server.testing.TestingUtil.clientBuilder;
import static java.util.Objects.requireNonNull;

public class TestingS3ClientProvider
        implements Provider<S3Client>
{
    private final Credential testingCredentials;
    private final TestingHttpServer httpServer;

    @Inject
    public TestingS3ClientProvider(
            TestingHttpServer httpServer,
            @ForTesting Credentials testingCredentials)
    {
        this.httpServer = requireNonNull(httpServer, "httpServer is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null").emulated();
    }

    @Override
    public S3Client get()
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(testingCredentials.accessKey(), testingCredentials.secretKey());

        return clientBuilder(httpServer)
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }
}
