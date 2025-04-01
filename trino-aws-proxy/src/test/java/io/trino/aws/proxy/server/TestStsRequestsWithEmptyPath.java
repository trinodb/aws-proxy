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
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;

@TrinoAwsProxyTest(filters = TestStsRequestsWithEmptyPath.Filter.class)
public class TestStsRequestsWithEmptyPath
        extends AbstractTestStsRequests
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withProperty("aws.proxy.sts.path", "");
        }
    }

    @Inject
    public TestStsRequestsWithEmptyPath(@ForTesting IdentityCredential testingCredentials, TestingHttpServer httpServer, CredentialsProvider credentialsProvider,
            TrinoAwsProxyConfig s3ProxyConfig)
    {
        super(testingCredentials, httpServer, credentialsProvider, s3ProxyConfig);
    }
}
