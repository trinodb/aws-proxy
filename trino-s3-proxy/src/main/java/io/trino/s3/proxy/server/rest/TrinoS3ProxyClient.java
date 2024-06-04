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
import io.trino.s3.proxy.server.security.SecurityController;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.commons.httpclient.ChunkedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.trino.s3.proxy.server.credentials.SigningController.formatRequestInstant;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyClient
{
    private static final int CHUNK_SIZE = 8_192 * 8;

    private final HttpClient httpClient;
    private final SigningController signingController;
    private final RemoteS3Facade remoteS3Facade;
    private final SecurityController securityController;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForProxyClient {}

    @Inject
    public TrinoS3ProxyClient(@ForProxyClient HttpClient httpClient, SigningController signingController, RemoteS3Facade remoteS3Facade, SecurityController securityController)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.remoteS3Facade = requireNonNull(remoteS3Facade, "objectStore is null");
        this.securityController = requireNonNull(securityController, "securityController is null");
    }

    @PreDestroy
    public void shutDown()
    {
        if (!shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30))) {
            // TODO add logging - check for false result
        }
    }

    public void proxyRequest(SigningMetadata signingMetadata, ParsedS3Request request, AsyncResponse asyncResponse)
    {
        URI remoteUri = remoteS3Facade.buildEndpoint(uriBuilder(request.queryParameters()), request.keyInBucket(), request.bucketName(), signingMetadata.region());

        // TODO log/expose any securityController error
        if (!securityController.apply(request, signingMetadata).canProceed()) {
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        Request.Builder remoteRequestBuilder = new Request.Builder()
                .setMethod(request.httpVerb())
                .setUri(remoteUri)
                .setFollowRedirects(true);

        if (remoteUri.getHost() == null) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        MultivaluedMap<String, String> remoteRequestHeaders = new MultivaluedHashMap<>();
        request.lowercaseHeaders().forEach((key, value) -> {
            switch (key) {
                case "x-amz-security-token" -> {}  // we add this below
                case "authorization" -> {} // we will create our own authorization header
                case "amz-sdk-invocation-id", "amz-sdk-request", "x-amz-decoded-content-length", "content-length", "content-encoding" -> {}   // don't send these
                case "x-amz-date" -> remoteRequestHeaders.putSingle("X-Amz-Date", formatRequestInstant(Instant.now())); // use now for the remote request
                case "host" -> remoteRequestHeaders.putSingle("Host", buildRemoteHost(remoteUri)); // replace source host with the remote AWS host
                default -> remoteRequestHeaders.put(key, value);
            }
        });

        signingMetadata.credentials()
                .requiredRemoteCredential()
                .session()
                .ifPresent(sessionToken -> remoteRequestHeaders.add("x-amz-security-token", sessionToken));

        request.entitySupplier().ifPresent(entitySupplier -> {
            InputStream entityStream;
            if ("aws-chunked".equals(request.lowercaseHeaders().getFirst("content-encoding"))) {
                // AWS's custom chunked encoding doesn't get handled by Jersey. Do it manually.
                // TODO move this into a Jersey MessageBodyReader
                try {
                    entityStream = new ChunkedInputStream(entitySupplier.get());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            else {
                entityStream = entitySupplier.get();
            }

            // TODO we need to add a Jersey MessageBodyWriter that handles aws-chunked
            remoteRequestBuilder.setBodyGenerator(new StreamingBodyGenerator(entityStream));
            remoteRequestHeaders.putSingle("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        });

        // set the new signed request auth header
        String signature = signingController.signRequest(
                signingMetadata,
                Credentials::requiredRemoteCredential,
                remoteUri,
                remoteRequestHeaders,
                request.queryParameters(),
                request.httpVerb(),
                Optional.empty());
        remoteRequestHeaders.putSingle("Authorization", signature);

        // remoteRequestHeaders now has correct values, copy to the remote request
        remoteRequestHeaders.forEach((name, values) -> values.forEach(value -> remoteRequestBuilder.addHeader(name, value)));

        Request remoteRequest = remoteRequestBuilder.build();

        executorService.submit(() -> {
            try {
                httpClient.execute(remoteRequest, new StreamingResponseHandler(asyncResponse));
            }
            catch (Throwable e) {
                asyncResponse.resume(e);
            }
        });
    }

    private static String buildRemoteHost(URI remoteUri)
    {
        int port = remoteUri.getPort();
        if ((port < 0) || (port == 80) || (port == 443)) {
            return remoteUri.getHost();
        }
        return remoteUri.getHost() + ":" + port;
    }

    private static UriBuilder uriBuilder(MultivaluedMap<String, String> queryParameters)
    {
        UriBuilder uriBuilder = UriBuilder.newInstance();
        queryParameters.forEach((name, values) -> uriBuilder.queryParam(name, values.toArray()));
        return uriBuilder;
    }
}
