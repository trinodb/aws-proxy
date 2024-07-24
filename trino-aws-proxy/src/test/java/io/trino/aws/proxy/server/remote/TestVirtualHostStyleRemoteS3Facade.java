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
package io.trino.aws.proxy.server.remote;

import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVirtualHostStyleRemoteS3Facade
{
    @Test
    public void testBuildEndpoint()
    {
        RemoteS3Config remoteS3Config = new RemoteS3Config().setHttps(false).setDomain("testS3Domain.com").setPort(80);
        RemoteS3Facade remoteS3Facade = new VirtualHostStyleRemoteS3Facade(remoteS3Config);
        URI expectedEndpoint = UriBuilder.fromUri("http://test_bucket.s3.us-east-1.testS3Domain.com:80/object_path/foo").build();
        URI actual = remoteS3Facade.buildEndpoint(UriBuilder.newInstance(), "object_path/foo", "test_bucket", "us-east-1");
        assertThat(actual).isEqualTo(expectedEndpoint);
    }

    @Test
    public void testBuildEndpointWithoutRegion()
    {
        RemoteS3Config remoteS3Config = new RemoteS3Config().setHttps(true).setDomain("testS3Domain.com").setPort(80).setHostnameTemplate("${bucket}.s3.${domain}");
        RemoteS3Facade remoteS3Facade = new VirtualHostStyleRemoteS3Facade(remoteS3Config);
        URI expectedEndpoint = UriBuilder.fromUri("https://test_bucket.s3.testS3Domain.com:80/object_path/foo").build();
        URI actual = remoteS3Facade.buildEndpoint(UriBuilder.newInstance(), "object_path/foo", "test_bucket", "us-east-1");
        assertThat(actual).isEqualTo(expectedEndpoint);
    }

    @Test
    public void testBuildEndpointWithEmptyBucketTemplate1()
    {
        RemoteS3Config remoteS3Config = new RemoteS3Config().setHttps(true).setDomain("testS3Domain.com").setPort(80).setHostnameTemplate("${bucket}.s3.${region}.${domain}");
        RemoteS3Facade remoteS3Facade = new VirtualHostStyleRemoteS3Facade(remoteS3Config);
        URI expectedEndpoint = UriBuilder.fromUri("https://s3.us-east-1.testS3Domain.com:80").build();
        URI actual = remoteS3Facade.buildEndpoint(UriBuilder.newInstance(), "", "", "us-east-1");
        assertThat(actual).isEqualTo(expectedEndpoint);
    }

    @Test
    public void testBuildEndpointWithEmptyBucketTemplate2()
    {
        RemoteS3Config remoteS3Config = new RemoteS3Config().setHttps(true).setDomain("testS3Domain.com").setPort(80).setHostnameTemplate("s3.${bucket}.${region}.${domain}");
        RemoteS3Facade remoteS3Facade = new VirtualHostStyleRemoteS3Facade(remoteS3Config);
        URI expectedEndpoint = UriBuilder.fromUri("https://s3.us-east-1.testS3Domain.com:80").build();
        URI actual = remoteS3Facade.buildEndpoint(UriBuilder.newInstance(), "", "", "us-east-1");
        assertThat(actual).isEqualTo(expectedEndpoint);
    }
}
