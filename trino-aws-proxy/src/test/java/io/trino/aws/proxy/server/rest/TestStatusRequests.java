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
package io.trino.aws.proxy.server.rest;

import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = WithTestingHttpClient.class)
public class TestStatusRequests
{
    private final NodeInfo nodeInfo;
    private final URI statusURI;
    private final HttpClient httpClient;
    private final JsonCodec<NodeStatus> jsonCodec;

    @Inject
    public TestStatusRequests(NodeInfo nodeInfo, TrinoAwsProxyConfig config, TestingHttpServer server, @ForTesting HttpClient httpClient)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.statusURI = UriBuilder.fromUri(server.getBaseUrl()).path(config.getStatusPath()).build();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonCodec = JsonCodec.jsonCodec(NodeStatus.class);
    }

    @Test
    public void testNodeStatus()
    {
        JsonResponse<NodeStatus> response = httpClient.execute(Request.builder().setMethod("GET").setUri(statusURI).build(), createFullJsonResponseHandler(jsonCodec));
        assertThat(response.getStatusCode()).isEqualTo(200);
        NodeStatus nodeStatus = response.getValue();
        assertThat(nodeStatus).isNotNull();
        assertThat(nodeStatus.nodeId()).isEqualTo(nodeInfo.getNodeId());
        assertThat(nodeStatus.environment()).isEqualTo(nodeInfo.getEnvironment());

        assertThat(httpClient.execute(Request.builder().setMethod("HEAD").setUri(statusURI).build(), createStatusResponseHandler())).extracting(StatusResponse::getStatusCode).isEqualTo(200);
    }
}
