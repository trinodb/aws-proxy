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
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.CredentialsProvider;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.s3.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.s3.proxy.server.testing.harness.BuilderFilter;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;

@TrinoS3ProxyTest(filters = TestStsRequestsWithEmptyPath.Filter.class)
public class TestStsRequestsWithEmptyPath
        extends AbstractTestStsRequests
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.withProperty("s3proxy.sts.path", "");
        }
    }

    @Inject
    public TestStsRequestsWithEmptyPath(@ForTesting Credentials testingCredentials, TestingHttpServer httpServer, CredentialsProvider credentialsProvider, TrinoS3ProxyConfig s3ProxyConfig)
    {
        super(testingCredentials, httpServer, credentialsProvider, s3ProxyConfig);
    }
}
