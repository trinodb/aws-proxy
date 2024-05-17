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
package io.trino.s3.proxy.server.rest;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningMetadata;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyClient
{
    private final HttpClient httpClient;
    private final SigningController signingController;
    private final S3EndpointBuilder endpointBuilder;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForProxyClient {}

    @Inject
    public TrinoS3ProxyClient(@ForProxyClient HttpClient httpClient, SigningController signingController, S3EndpointBuilder endpointBuilder)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.endpointBuilder = requireNonNull(endpointBuilder, "endpointBuilder is null");
    }

    @PreDestroy
    public void shutDown()
    {
        if (!shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30))) {
            // TODO add logging - check for false result
        }
    }

    public void proxyRequest(SigningMetadata signingMetadata, ContainerRequest request, AsyncResponse asyncResponse, String bucket)
    {
        String realPath = rewriteRequestPath(request, bucket);

        URI realUri = endpointBuilder.buildEndpoint(request.getUriInfo().getRequestUriBuilder(), realPath, bucket, signingMetadata.region());

        Request.Builder realRequestBuilder = new Request.Builder()
                .setMethod(request.getMethod())
                .setUri(realUri)
                .setFollowRedirects(true);

        if (realUri.getHost() == null) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        // modify the source request headers for real values needed for the real AWS request
        MultivaluedMap<String, String> realRequestHeaders = new MultivaluedHashMap<>(request.getRequestHeaders());
        // we don't use sessions when making the real AWS call
        realRequestHeaders.remove("X-Amz-Security-Token");
        // replace source host with the real AWS host
        realRequestHeaders.putSingle("Host", realUri.getHost());

        // set the new signed request auth header
        String encodedPath = firstNonNull(realUri.getRawPath(), "");
        String signature = signingController.signRequest(
                signingMetadata,
                Credentials::real,
                realUri,
                realRequestHeaders,
                request.getUriInfo().getQueryParameters(),
                request.getMethod(),
                encodedPath);
        realRequestHeaders.putSingle("Authorization", signature);

        // requestHeaders now has correct values, copy to the real request
        realRequestHeaders.forEach((name, values) -> values.forEach(value -> realRequestBuilder.addHeader(name, value)));

        Request realRequest = realRequestBuilder.build();

        // TODO config a max time to wait
        executorService.submit(() -> httpClient.execute(realRequest, new StreamingResponseHandler(asyncResponse, Duration.ofHours(1))));
    }

    private static String rewriteRequestPath(ContainerRequest request, String bucket)
    {
        String path = "/" + request.getPath(false);
        if (!path.startsWith(TrinoS3ProxyRestConstants.S3_PATH)) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        path = path.substring(TrinoS3ProxyRestConstants.S3_PATH.length());
        if (path.startsWith("/" + bucket)) {
            path = path.substring(("/" + bucket).length());
        }

        if (path.isEmpty() && bucket.isEmpty()) {
            path = "/";
        }

        return path;
    }
}
