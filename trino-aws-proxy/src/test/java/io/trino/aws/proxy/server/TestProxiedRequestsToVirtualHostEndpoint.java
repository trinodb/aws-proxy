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
import io.trino.aws.proxy.server.remote.DefaultRemoteS3Config;
import io.trino.aws.proxy.server.remote.VirtualHostStyleRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import software.amazon.awssdk.services.s3.S3Client;

import static io.trino.aws.proxy.server.testing.TestingUtil.LOCALHOST_DOMAIN;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestProxiedRequestsToVirtualHostEndpoint
        extends AbstractTestProxiedRequests
{
    @Inject
    public TestProxiedRequestsToVirtualHostEndpoint(S3Client s3Client, @ForS3Container S3Client storageClient, TestingS3RequestRewriteController requestRewriteController,
            TestingRemoteS3Facade testingRemoteS3Facade, S3Container s3Container)
    {
        super(s3Client, storageClient, requestRewriteController);

        testingRemoteS3Facade.setDelegate(new VirtualHostStyleRemoteS3Facade(new DefaultRemoteS3Config()
                .setDomain(LOCALHOST_DOMAIN)
                .setPort(s3Container.containerHost().getPort())
                .setHttps(false)
                .setVirtualHostStyle(true)
                .setHostnameTemplate("${bucket}.${domain}")));
    }
}
