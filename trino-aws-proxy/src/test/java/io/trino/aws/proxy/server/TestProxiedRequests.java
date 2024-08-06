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
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import software.amazon.awssdk.services.s3.S3Client;

@TrinoAwsProxyTest(filters = {WithConfiguredBuckets.class, TestProxiedRequests.Filter.class})
public class TestProxiedRequests
        extends AbstractTestProxiedRequests
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withProperty("aws.proxy.s3.path", "/api/some/s3/path");
        }
    }

    @Inject
    public TestProxiedRequests(S3Client s3Client, @ForS3Container S3Client storageClient, TestingS3RequestRewriteController requestRewriteController)
    {
        super(s3Client, storageClient, requestRewriteController);
    }
}
