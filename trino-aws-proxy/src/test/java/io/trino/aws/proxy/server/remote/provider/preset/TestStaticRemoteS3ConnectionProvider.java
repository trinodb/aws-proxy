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
package io.trino.aws.proxy.server.remote.provider.preset;

import com.google.inject.Inject;
import io.trino.aws.proxy.server.AbstractTestProxiedRequests;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import software.amazon.awssdk.services.s3.S3Client;

import static io.trino.aws.proxy.server.testing.TestingUtil.TESTING_REMOTE_CREDENTIAL;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestStaticRemoteS3ConnectionProvider
        extends AbstractTestProxiedRequests
{
    public static final class Filter
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            return builder
                    .withoutTestingRemoteS3ConnectionProvider()
                    .withProperty("remote-s3-connection-provider.type", "static")
                    .withProperty("remote-s3-connection-provider.access-key", TESTING_REMOTE_CREDENTIAL.accessKey())
                    .withProperty("remote-s3-connection-provider.secret-key", TESTING_REMOTE_CREDENTIAL.secretKey());
        }
    }

    @Inject
    public TestStaticRemoteS3ConnectionProvider(S3Client s3Client, @ForS3Container S3Client storageClient, TestingS3RequestRewriteController requestRewriteController)
    {
        super(s3Client, storageClient, requestRewriteController);
    }
}
