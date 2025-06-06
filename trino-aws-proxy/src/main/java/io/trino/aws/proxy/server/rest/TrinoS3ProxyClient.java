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
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.remote.RemoteS3ConnectionController;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.S3RequestRewriter;
import io.trino.aws.proxy.spi.rest.S3RequestRewriter.S3RewriteResult;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.security.SecurityResponse.Failure;
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
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.InputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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

    private final HttpClient httpClient;
    private final SigningController signingController;
    private final S3SecurityController s3SecurityController;
    private final S3PresignController s3PresignController;
    private final LimitStreamController limitStreamController;
    private final S3RequestRewriter s3RequestRewriter;
    private final RemoteS3ConnectionController remoteS3ConnectionController;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final boolean generatePresignedUrlsOnHead;

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForProxyClient
    {
    }

    @Inject
    public TrinoS3ProxyClient(
            @ForProxyClient HttpClient httpClient,
            SigningController signingController,
            S3SecurityController s3SecurityController,
            TrinoAwsProxyConfig trinoAwsProxyConfig,
            S3PresignController s3PresignController,
            LimitStreamController limitStreamController,
            S3RequestRewriter s3RequestRewriter,
            RemoteS3ConnectionController remoteS3ConnectionController)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.s3SecurityController = requireNonNull(s3SecurityController, "securityController is null");
        this.s3PresignController = requireNonNull(s3PresignController, "presignController is null");
        this.limitStreamController = requireNonNull(limitStreamController, "quotaStreamController is null");
        this.s3RequestRewriter = requireNonNull(s3RequestRewriter, "s3RequestRewriter is null");
        this.remoteS3ConnectionController = requireNonNull(remoteS3ConnectionController, "remoteS3ConnectionController is null");

        generatePresignedUrlsOnHead = trinoAwsProxyConfig.isGeneratePresignedUrlsOnHead();
    }

    @PreDestroy
    public void shutDown()
    {
        if (!shutdownAndAwaitTermination(executorService, Duration.ofSeconds(30))) {
            log.warn("Could not shutdown executor service");
        }
    }

    public void proxyRequest(Optional<Identity> identity, SigningMetadata signingMetadata, ParsedS3Request request, AsyncResponse asyncResponse,
            RequestLoggingSession requestLoggingSession)
    {
        SecurityResponse securityResponse = s3SecurityController.apply(request, identity);
        if (securityResponse instanceof Failure(var error)) {
            log.debug("SecurityController check failed. AccessKey: %s, Request: %s, SecurityResponse: %s", signingMetadata.credential().accessKey(), request, securityResponse);
            requestLoggingSession.logError("request.security.fail.credentials", signingMetadata.credential());
            requestLoggingSession.logError("request.security.fail.request", request);
            requestLoggingSession.logError("request.security.fail.error", error);

            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        Optional<S3RewriteResult> rewriteResult = s3RequestRewriter.rewrite(identity, signingMetadata, request);
        String targetBucket = rewriteResult.map(S3RewriteResult::finalRequestBucket).orElse(request.bucketName());
        String targetKey = rewriteResult
                .map(S3RewriteResult::finalRequestKey)
                .map(SdkHttpUtils::urlEncodeIgnoreSlashes)
                .orElse(request.rawPath());

        RemoteRequestWithPresignedURIs remoteRequest = remoteS3ConnectionController.withRemoteConnection(signingMetadata, identity, request, (remoteCredential, remoteS3Facade) -> {
            URI remoteUri = remoteS3Facade.buildEndpoint(uriBuilder(request.queryParameters()), targetKey, targetBucket, request.requestAuthorization().region());

            Request.Builder remoteRequestBuilder = new Request.Builder()
                    .setMethod(request.httpVerb())
                    .setUri(remoteUri)
                    .setFollowRedirects(true);

            if (remoteUri.getHost() == null) {
                log.debug("RemoteURI missing host. AccessKey: %s, Request: %s", signingMetadata.credential().accessKey(), request);
                throw new WebApplicationException(Response.Status.BAD_REQUEST);
            }

            ImmutableMultiMap.Builder remoteRequestHeadersBuilder = ImmutableMultiMap.builder(false);
            Instant targetRequestTimestamp = Instant.now();
            request.requestHeaders().passthroughHeaders().forEach(remoteRequestHeadersBuilder::addAll);
            remoteRequestHeadersBuilder.putOrReplaceSingle("Host", buildRemoteHost(remoteUri));

            // Use now for the remote request
            remoteRequestHeadersBuilder.putOrReplaceSingle("X-Amz-Date", AwsTimestamp.toRequestFormat(targetRequestTimestamp));

            request.requestContent().contentLength().ifPresent(length -> remoteRequestHeadersBuilder.putOrReplaceSingle("content-length", Integer.toString(length)));
            // All SigV4 requests require an x-amz-content-sha256
            remoteRequestHeadersBuilder.putOrReplaceSingle("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

            SigningMetadata remoteSigningMetadata = signingMetadata.withCredential(remoteCredential);

            Map<String, URI> presignedUrls;
            if (generatePresignedUrlsOnHead && request.httpVerb().equalsIgnoreCase("HEAD")) {
                presignedUrls = s3PresignController.buildPresignedRemoteUrls(identity, remoteSigningMetadata, request, targetRequestTimestamp, remoteUri);
            }
            else {
                presignedUrls = ImmutableMap.of();
            }

            remoteCredential
                    .session()
                    .ifPresent(sessionToken -> remoteRequestHeadersBuilder.putOrReplaceSingle("x-amz-security-token", sessionToken));

            contentInputStream(request.requestContent(), remoteSigningMetadata).ifPresent(inputStream -> remoteRequestBuilder.setBodyGenerator(streamingBodyGenerator(inputStream)));

            // set the new signed request auth header
            MultiMap remoteRequestHeaders = remoteRequestHeadersBuilder.build();
            String signature = signingController.signRequest(
                    remoteSigningMetadata,
                    request.requestAuthorization().region(),
                    targetRequestTimestamp,
                    Optional.empty(),
                    remoteUri,
                    remoteRequestHeaders,
                    request.queryParameters(),
                    request.httpVerb()).signingAuthorization().authorization();

            // remoteRequestHeaders now has correct values, copy to the remote request
            remoteRequestHeaders.forEachEntry(remoteRequestBuilder::addHeader);
            remoteRequestBuilder.addHeader("Authorization", signature);

            return new RemoteRequestWithPresignedURIs(remoteRequestBuilder.build(), presignedUrls);
        }).orElseThrow(() -> {
            requestLoggingSession.logError("request.remote.fail.resolution", "Failed to resolve remote");
            return new WebApplicationException(Response.Status.NOT_FOUND);
        });

        executorService.submit(() -> {
            StreamingResponseHandler responseHandler = new StreamingResponseHandler(asyncResponse, remoteRequest.presignedUrls(), requestLoggingSession, limitStreamController);
            try {
                httpClient.execute(remoteRequest.remoteRequest(), responseHandler);
            }
            catch (Throwable e) {
                // TODO: if responseHandler is null this will throw an NPE inside a catch clause, so the request doesn't terminate properly; fix; also we should have a timeout
                //  for request processing
                responseHandler.handleException(remoteRequest.remoteRequest(), new RuntimeException(e));
            }
        });
    }

    private Optional<InputStream> contentInputStream(RequestContent requestContent, SigningMetadata signingMetadata)
    {
        return switch (requestContent.contentType()) {
            case AWS_CHUNKED, AWS_CHUNKED_IN_W3C_CHUNKED -> requestContent.inputStream()
                    .map(inputStream -> new AwsChunkedInputStream(limitStreamController.wrap(inputStream), signingMetadata.requiredSigningContext().chunkSigningSession(), requestContent.contentLength().orElseThrow()));

            case STANDARD, W3C_CHUNKED -> requestContent.inputStream().map(inputStream -> {
                SigningContext signingContext = signingMetadata.requiredSigningContext();
                return signingContext.contentHash()
                        .filter(contentHash -> !contentHash.startsWith("STREAMING-") && !contentHash.startsWith("UNSIGNED-"))
                        .map(contentHash -> (InputStream) new HashCheckInputStream(limitStreamController.wrap(inputStream), contentHash, requestContent.contentLength()))
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

    private record RemoteRequestWithPresignedURIs(Request remoteRequest, Map<String, URI> presignedUrls)
    {
        private RemoteRequestWithPresignedURIs
        {
            requireNonNull(remoteRequest, "remoteRequest is null");
            requireNonNull(presignedUrls, "presignedUrls is null");
        }
    }
}
