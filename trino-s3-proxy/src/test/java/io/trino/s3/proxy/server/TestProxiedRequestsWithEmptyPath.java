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
import io.trino.s3.proxy.server.testing.ManagedS3MockContainer.ForS3MockContainer;
import io.trino.s3.proxy.server.testing.TestingTrinoS3ProxyServer;
import io.trino.s3.proxy.server.testing.harness.BuilderFilter;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithConfiguredBuckets;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

@TrinoS3ProxyTest(filters = {WithConfiguredBuckets.class, TestProxiedRequestsWithEmptyPath.Filter.class})
public class TestProxiedRequestsWithEmptyPath
        extends AbstractTestProxiedRequests
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoS3ProxyServer.Builder filter(TestingTrinoS3ProxyServer.Builder builder)
        {
            return builder.withProperty("s3proxy.s3.path", "");
        }
    }

    @Inject
    public TestProxiedRequestsWithEmptyPath(S3Client s3Client, @ForS3MockContainer S3Client storageClient, @ForS3MockContainer List<String> configuredBuckets)
    {
        super(s3Client, storageClient, configuredBuckets);
    }
}
