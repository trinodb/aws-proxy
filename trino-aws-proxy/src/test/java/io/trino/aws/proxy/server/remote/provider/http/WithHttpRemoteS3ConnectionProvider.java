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
package io.trino.aws.proxy.server.remote.provider.http;

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;

import static io.trino.aws.proxy.server.testing.TestingUtil.createTestingHttpServer;

public final class WithHttpRemoteS3ConnectionProvider
        implements BuilderFilter
{
    @Override
    public Builder filter(Builder builder)
    {
        TestingHttpRemoteS3ConnectionProviderServlet remoteS3ConnectionProviderServlet;
        URI httpEndpointUri;
        try {
            remoteS3ConnectionProviderServlet = new TestingHttpRemoteS3ConnectionProviderServlet();
            TestingHttpServer server = createTestingHttpServer(remoteS3ConnectionProviderServlet);
            server.start();
            httpEndpointUri = server.getBaseUrl();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start test http remote s3 connection provider server", e);
        }

        return builder.withoutTestingRemoteS3ConnectionProvider()
                .withProperty("remote-s3-connection-provider.type", "http")
                .withProperty("remote-s3-connection-provider.http.endpoint", UriBuilder.fromUri(httpEndpointUri).path("/api/v1/remote_s3_connection").build().toString())
                .withProperty("remote-s3-connection-provider.http.cache-size", "100")
                .withProperty("remote-s3-connection-provider.http.cache-ttl", "5m")
                .addModule(binder -> binder.bind(TestingHttpRemoteS3ConnectionProviderServlet.class).toInstance(remoteS3ConnectionProviderServlet));
    }
}
