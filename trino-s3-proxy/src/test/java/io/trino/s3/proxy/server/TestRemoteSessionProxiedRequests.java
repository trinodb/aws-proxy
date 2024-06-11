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

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServerModule.ForTestingRemoteCredentials;
import io.trino.s3.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules;
import io.trino.s3.proxy.spi.credentials.Credentials;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

import static io.trino.s3.proxy.server.testing.TestingUtil.clientBuilder;

@TrinoS3ProxyTest(filters = TrinoS3ProxyTestCommonModules.WithConfiguredBuckets.class)
public class TestRemoteSessionProxiedRequests
        extends AbstractTestProxiedRequests
{
    @Inject
    public TestRemoteSessionProxiedRequests(@ForS3Container S3Client storageClient, @ForTestingRemoteCredentials Credentials remoteCredentials, TestingHttpServer httpServer, @ForS3Container List<String> configuredBuckets)
    {
        super(buildInternalClient(remoteCredentials, httpServer), storageClient, configuredBuckets);
    }

    private static S3Client buildInternalClient(Credentials credentials, TestingHttpServer httpServer)
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(credentials.emulated().accessKey(), credentials.emulated().secretKey());

        return clientBuilder(httpServer.getBaseUrl())
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }
}
