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
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
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
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.trino.s3.proxy.server.credentials.SigningController.formatRequestInstant;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyClient
{
    private final HttpClient httpClient;
    private final SigningController signingController;
    private final RemoteS3Facade remoteS3Facade;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForProxyClient {}

    @Inject
    public TrinoS3ProxyClient(@ForProxyClient HttpClient httpClient, SigningController signingController, RemoteS3Facade remoteS3Facade)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.remoteS3Facade = requireNonNull(remoteS3Facade, "objectStore is null");
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

        URI realUri = remoteS3Facade.buildEndpoint(request.getUriInfo().getRequestUriBuilder(), realPath, bucket, signingMetadata.region());

        Request.Builder realRequestBuilder = new Request.Builder()
                .setMethod(request.getMethod())
                .setUri(realUri)
                .setFollowRedirects(true);

        if (realUri.getHost() == null) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        MultivaluedMap<String, String> realRequestHeaders = new MultivaluedHashMap<>();
        request.getRequestHeaders().forEach((key, value) -> {
            switch (key.toLowerCase()) {
                case "x-amz-security-token" -> {}   // we don't use sessions when making the real AWS call
                case "amz-sdk-invocation-id", "amz-sdk-request" -> {}   // don't send these
                case "x-amz-date" -> realRequestHeaders.putSingle("X-Amz-Date", formatRequestInstant(Instant.now())); // use now for the real request
                case "host" -> realRequestHeaders.putSingle("Host", buildRealHost(realUri)); // replace source host with the real AWS host
                default -> realRequestHeaders.put(key, value);
            }
        });

        // set the new signed request auth header
        String encodedPath = firstNonNull(realUri.getRawPath(), "");
        String signature = signingController.signRequest(
                signingMetadata,
                Credentials::requiredRealCredential,
                realUri,
                realRequestHeaders,
                request.getUriInfo().getQueryParameters(),
                request.getMethod());
        realRequestHeaders.putSingle("Authorization", signature);

        // requestHeaders now has correct values, copy to the real request
        realRequestHeaders.forEach((name, values) -> values.forEach(value -> realRequestBuilder.addHeader(name, value)));

        Request realRequest = realRequestBuilder.build();

        executorService.submit(() -> httpClient.execute(realRequest, new StreamingResponseHandler(asyncResponse)));
    }

    private static String buildRealHost(URI realUri)
    {
        int port = realUri.getPort();
        if ((port < 0) || (port == 80) || (port == 443)) {
            return realUri.getHost();
        }
        return realUri.getHost() + ":" + port;
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
