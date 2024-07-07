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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.rest.TestHangingStreamingResponseHandler.ForTimeout;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriInfo;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

@Path("/")
public class HangingResource
{
    private final HttpClient httpClient;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @Inject
    public HangingResource(@ForTimeout HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @PreDestroy
    public void shutDown()
    {
        shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30));
    }

    @GET
    public void callHangingRequest(@Context UriInfo uriInfo, @Suspended AsyncResponse asyncResponse)
    {
        // simulate calling a remote request and streaming the result while the remote server hangs
        Request request = prepareGet().setUri(uriInfo.getBaseUri().resolve("hang")).build();
        httpClient.execute(request, new StreamingResponseHandler(asyncResponse, ImmutableMap.of(), () -> {}, new LimitStreamController(new TrinoAwsProxyConfig())));
    }

    @GET
    @Path("/hang")
    public void hang(@Suspended AsyncResponse asyncResponse)
    {
        StreamingOutput streamingOutput = output -> {
            // simulate the start of a JSON string
            output.write('"');

            // write enough bytes so that the StreamingResponseHandler gets called
            byte[] bytes = new byte[16384];
            Arrays.fill(bytes, (byte) ' ');
            output.write(bytes);
            output.flush();

            // simulate a hanging remote request by never returning
            try {
                Thread.currentThread().join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Response response = Response.ok(streamingOutput)
                .header(CONTENT_TYPE, APPLICATION_JSON_TYPE)
                .header(CONTENT_LENGTH, "99999999")
                .build();
        asyncResponse.resume(response);
    }
}
