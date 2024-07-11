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

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.signing.SigningContext;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
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
import static io.airlift.http.client.StreamingBodyGenerator.streamingBodyGenerator;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyClient
{
    private static final Logger log = Logger.get(TrinoS3ProxyClient.class);

    private static final int CHUNK_SIZE = 8_192 * 8;

    private final HttpClient httpClient;
    private final SigningController signingController;
    private final RemoteS3Facade remoteS3Facade;
    private final S3SecurityController s3SecurityController;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForProxyClient {}

    @Inject
    public TrinoS3ProxyClient(@ForProxyClient HttpClient httpClient, SigningController signingController, RemoteS3Facade remoteS3Facade, S3SecurityController s3SecurityController)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.remoteS3Facade = requireNonNull(remoteS3Facade, "objectStore is null");
        this.s3SecurityController = requireNonNull(s3SecurityController, "securityController is null");
    }

    @PreDestroy
    public void shutDown()
    {
        if (!shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30))) {
            log.warn("Could not shutdown executor service");
        }
    }

    public void proxyRequest(SigningMetadata signingMetadata, ParsedS3Request request, AsyncResponse asyncResponse, RequestLoggingSession requestLoggingSession)
    {
        URI remoteUri = remoteS3Facade.buildEndpoint(uriBuilder(request.queryParameters()), request.rawPath(), request.bucketName(), request.requestAuthorization().region());

        SecurityResponse securityResponse = s3SecurityController.apply(request);
        if (!securityResponse.canProceed()) {
            log.debug("SecurityController check failed. AccessKey: %s, Request: %s, SecurityResponse: %s", signingMetadata.credentials().emulated().accessKey(), request, securityResponse);
            requestLoggingSession.logError("request.security.fail.credentials", signingMetadata.credentials().emulated());
            requestLoggingSession.logError("request.security.fail.request", request);
            requestLoggingSession.logError("request.security.fail.response", securityResponse);

            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        Request.Builder remoteRequestBuilder = new Request.Builder()
                .setMethod(request.httpVerb())
                .setUri(remoteUri)
                .setFollowRedirects(true);

        if (remoteUri.getHost() == null) {
            log.debug("RemoteURI missing host. AccessKey: %s, Request: %s", signingMetadata.credentials().emulated().accessKey(), request);
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        ImmutableMultiMap.Builder remoteRequestHeadersBuilder = ImmutableMultiMap.builder(false);
        Instant targetRequestTimestamp = Instant.now();
        request.requestHeaders().forEach((headerName, headerValues) -> {
            switch (headerName) {
                case "x-amz-security-token" -> {}  // we add this below
                case "authorization" -> {} // we will create our own authorization header
                case "amz-sdk-invocation-id", "amz-sdk-request", "x-amz-decoded-content-length", "content-length", "content-encoding" -> {}   // don't send these
                case "x-amz-date" -> {} // we will add our own later
                case "host" -> remoteRequestHeadersBuilder.putOrReplaceSingle("Host", buildRemoteHost(remoteUri)); // replace source host with the remote AWS host
                default -> remoteRequestHeadersBuilder.addAll(headerName, headerValues);
            }
        });

        // Use now for the remote request
        remoteRequestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", AwsTimestamp.toRequestFormat(targetRequestTimestamp));

        signingMetadata.credentials()
                .requiredRemoteCredential()
                .session()
                .ifPresent(sessionToken -> remoteRequestHeadersBuilder.putOrReplaceSingle("x-amz-security-token", sessionToken));

        request.requestContent().contentLength().ifPresent(length -> remoteRequestHeadersBuilder.putOrReplaceSingle("content-length", Integer.toString(length)));

        contentInputStream(request.requestContent(), signingMetadata).ifPresent(inputStream -> remoteRequestBuilder.setBodyGenerator(streamingBodyGenerator(inputStream)));
        // All SigV4 requests require an x-amz-content-sha256
        remoteRequestHeadersBuilder.putOrReplaceSingle("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

        // set the new signed request auth header
        MultiMap remoteRequestHeaders = remoteRequestHeadersBuilder.build();
        String signature = signingController.signRequest(
                signingMetadata,
                request.requestAuthorization().region(),
                targetRequestTimestamp,
                Optional.empty(),
                Credentials::requiredRemoteCredential,
                remoteUri,
                remoteRequestHeaders,
                request.queryParameters(),
                request.httpVerb()).authorization();

        // remoteRequestHeaders now has correct values, copy to the remote request
        remoteRequestHeaders.forEachEntry(remoteRequestBuilder::addHeader);
        remoteRequestBuilder.addHeader("Authorization", signature);

        Request remoteRequest = remoteRequestBuilder.build();

        executorService.submit(() -> {
            StreamingResponseHandler responseHandler = new StreamingResponseHandler(asyncResponse, requestLoggingSession);
            try {
                httpClient.execute(remoteRequest, responseHandler);
            }
            catch (Throwable e) {
                responseHandler.handleException(remoteRequest, new RuntimeException(e));
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

    private static UriBuilder uriBuilder(MultiMap queryParameters)
    {
        UriBuilder uriBuilder = UriBuilder.newInstance();
        queryParameters.forEachEntry(uriBuilder::queryParam);
        return uriBuilder;
    }
}
