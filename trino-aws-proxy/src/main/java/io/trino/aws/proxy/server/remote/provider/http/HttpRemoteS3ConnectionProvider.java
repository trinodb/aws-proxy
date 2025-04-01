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

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static java.util.Objects.requireNonNull;

public class HttpRemoteS3ConnectionProvider
        implements RemoteS3ConnectionProvider
{
    private final HttpClient httpClient;
    private final JsonCodec<RemoteS3ConnectionRequest> requestCodec;
    private final JsonCodec<RemoteS3Connection> responseCodec;
    private final URI endpoint;

    public HttpRemoteS3ConnectionProvider(
            @ForHttpRemoteS3ConnectionProvider HttpClient httpClient,
            HttpRemoteS3ConnectionProviderConfig config,
            JsonCodec<RemoteS3ConnectionRequest> requestCodec,
            JsonCodec<RemoteS3Connection> responseCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.requestCodec = requireNonNull(requestCodec, "requestCodec is null");
        this.responseCodec = requireNonNull(responseCodec, "responseCodec is null");
        this.endpoint = config.getEndpoint();
    }

    @Override
    public Optional<RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
    {
        RemoteS3ConnectionRequest body = new RemoteS3ConnectionRequest(signingMetadata, identity, request);
        JsonResponse<RemoteS3Connection> response = httpClient.execute(
                preparePost()
                        .setUri(endpoint)
                        .setBodyGenerator(createStaticBodyGenerator(requestCodec.toJsonBytes(body))).build(),
                createFullJsonResponseHandler(responseCodec));
        var statusCode = HttpStatus.fromStatusCode(response.getStatusCode());
        if (statusCode.family() != HttpStatus.Family.SUCCESSFUL) {
            if (statusCode == HttpStatus.NOT_FOUND) {
                return Optional.empty();
            }
            throw new RuntimeException("Failed to get remote S3 connection with HTTP plugin. Response code: " + statusCode + "; body: \n" + response.getResponseBody());
        }
        if (!response.hasValue()) {
            throw new RuntimeException("Failed to get remote S3 connection with HTTP plugin. Response code: " + statusCode + "; no body");
        }
        return Optional.of(response.getValue());
    }
}
