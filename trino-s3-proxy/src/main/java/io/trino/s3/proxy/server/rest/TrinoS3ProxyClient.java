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
import io.trino.s3.proxy.server.remote.RemoteS3Facade;
import io.trino.s3.proxy.server.security.SecurityController;
import io.trino.s3.proxy.server.signing.SigningContext;
import io.trino.s3.proxy.server.signing.SigningController;
import io.trino.s3.proxy.server.signing.SigningMetadata;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import java.io.InputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.trino.s3.proxy.server.signing.SigningController.formatRequestInstant;
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
        URI remoteUri = remoteS3Facade.buildEndpoint(uriBuilder(request.queryParameters()), request.rawPath(), request.bucketName(), request.requestAuthorization().region());

        // TODO log/expose any securityController error
        if (!securityController.apply(request).canProceed()) {
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
        String targetXAmzDate = formatRequestInstant(Instant.now());
        request.lowercaseHeaders().forEach((key, value) -> {
            switch (key) {
                case "x-amz-security-token" -> {}  // we add this below
                case "authorization" -> {} // we will create our own authorization header
                case "amz-sdk-invocation-id", "amz-sdk-request", "x-amz-decoded-content-length", "content-length", "content-encoding" -> {}   // don't send these
                case "x-amz-date" -> remoteRequestHeaders.putSingle("X-Amz-Date", targetXAmzDate); // use now for the remote request
                case "host" -> remoteRequestHeaders.putSingle("Host", buildRemoteHost(remoteUri)); // replace source host with the remote AWS host
                default -> remoteRequestHeaders.put(key, value);
            }
        });

        signingMetadata.credentials()
                .requiredRemoteCredential()
                .session()
                .ifPresent(sessionToken -> remoteRequestHeaders.add("x-amz-security-token", sessionToken));

        request.requestContent().contentLength().ifPresent(length -> remoteRequestHeaders.putSingle("content-length", Integer.toString(length)));

        contentInputStream(request.requestContent(), signingMetadata).ifPresent(inputStream -> {
            remoteRequestBuilder.setBodyGenerator(new StreamingBodyGenerator(inputStream));
            remoteRequestHeaders.putSingle("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
        });

        // set the new signed request auth header
        String signature = signingController.signRequest(
                signingMetadata,
                request.requestAuthorization().region(),
                targetXAmzDate,
                Credentials::requiredRemoteCredential,
                remoteUri,
                remoteRequestHeaders,
                request.queryParameters(),
                request.httpVerb());
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

    private Optional<InputStream> contentInputStream(RequestContent requestContent, SigningMetadata signingMetadata)
    {
        return switch (requestContent.contentType()) {
            case AWS_CHUNKED -> requestContent.inputStream().map(inputStream -> new AwsChunkedInputStream(inputStream, Optional.of(signingMetadata.requiredSigningContext().chunkSigningSession())));

            case STANDARD, W3C_CHUNKED -> requestContent.inputStream().map(inputStream -> {
                SigningContext signingContext = signingMetadata.requiredSigningContext();
                return signingContext.contentHash()
                        .filter(contentHash -> !contentHash.startsWith("STREAMING-") && !contentHash.startsWith("UNSIGNED-"))
                        .map(contentHash -> (InputStream) new HashCheckInputStream(inputStream, contentHash, requestContent.contentLength()))
                        .orElse(inputStream);
            });

            case EMPTY -> Optional.empty();
        };
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
