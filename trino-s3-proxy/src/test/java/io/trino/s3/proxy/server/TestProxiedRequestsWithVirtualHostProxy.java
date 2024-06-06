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
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithVirtualHostEnabledProxy;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

@TrinoS3ProxyTest(filters = {WithConfiguredBuckets.class, WithVirtualHostEnabledProxy.class, WithVirtualHostEnabledProxy.class})
public class TestProxiedRequestsWithVirtualHostProxy
        extends AbstractTestProxiedRequests
{
    @Inject
    public TestProxiedRequestsWithVirtualHostProxy(S3Client s3Client, @ForS3MockContainer S3Client storageClient, @ForS3MockContainer List<String> configuredBuckets)
    {
        super(s3Client, storageClient, configuredBuckets);
    }
}
