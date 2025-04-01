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

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import software.amazon.awssdk.services.s3.S3Client;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, WithTestingHttpClient.class, TestProxiedRequests.Filter.class})
public class TestPresignedRequests
        extends AbstractTestPresignedRequests
{
    @Inject
    public TestPresignedRequests(
            @ForTesting HttpClient httpClient,
            S3Client internalClient,
            @ForS3Container S3Client storageClient,
            @ForTesting IdentityCredential testingCredentials,
            TestingHttpServer httpServer,
            TrinoAwsProxyConfig s3ProxyConfig,
            XmlMapper xmlMapper,
            TestingS3RequestRewriteController requestRewriteController)
    {
        super(httpClient, internalClient, storageClient, testingCredentials, httpServer, s3ProxyConfig, xmlMapper, requestRewriteController);
    }
}
