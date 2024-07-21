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
package io.trino.aws.proxy.server;

import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServerModule.ForTestingRemoteCredentials;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Credentials;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Optional;

import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestRemoteSessionProxiedRequests
        extends AbstractTestProxiedRequests
{
    @Inject
    public TestRemoteSessionProxiedRequests(@ForS3Container S3Client storageClient, @ForTestingRemoteCredentials Credentials remoteCredentials, TestingHttpServer httpServer, @ForS3Container List<String> configuredBuckets, TrinoAwsProxyConfig trinoAwsProxyConfig)
    {
        super(buildInternalClient(remoteCredentials, httpServer, trinoAwsProxyConfig.getS3Path()), storageClient, configuredBuckets);
    }

    private static S3Client buildInternalClient(Credentials credentials, TestingHttpServer httpServer, String s3Path)
    {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(credentials.emulated().accessKey(), credentials.emulated().secretKey());

        return clientBuilder(httpServer.getBaseUrl(), Optional.of(s3Path))
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }
}
