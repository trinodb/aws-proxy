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
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.SecurityResponse.Failure;
import io.trino.aws.proxy.spi.security.SecurityResponse.Success;
import io.trino.aws.proxy.spi.signing.SigningContext;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class S3PresignController
{
    private final SigningController signingController;
    private final Duration presignUrlDuration;
    private final S3SecurityController s3SecurityController;

    @Inject
    public S3PresignController(SigningController signingController, TrinoAwsProxyConfig trinoAwsProxyConfig, S3SecurityController s3SecurityController)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.s3SecurityController = requireNonNull(s3SecurityController, "s3SecurityController is null");

        presignUrlDuration = trinoAwsProxyConfig.getPresignedUrlsDuration().toJavaTime();
    }

    public Map<String, URI> buildPresignedRemoteUrls(SigningMetadata signingMetadata, ParsedS3Request request, Instant targetRequestTimestamp, URI remoteUri)
    {
        Optional<Instant> signatureExpiry = Optional.of(Instant.now().plusMillis(presignUrlDuration.toMillis()));

        return Stream.of("GET", "PUT", "POST", "DELETE")
                .flatMap(httpMethod -> buildPresignedRemoteUrl(httpMethod, signingMetadata, request, targetRequestTimestamp, remoteUri, signatureExpiry))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Stream<Map.Entry<String, URI>> buildPresignedRemoteUrl(String httpMethod, SigningMetadata signingMetadata, ParsedS3Request request, Instant targetRequestTimestamp, URI remoteUri, Optional<Instant> signatureExpiry)
    {
        SigningContext signingContext = signingController.presignRequest(
                signingMetadata,
                request.requestAuthorization().region(),
                targetRequestTimestamp,
                signatureExpiry,
                Credentials::requiredRemoteCredential,
                remoteUri,
                request.queryParameters(),
                httpMethod);

        // everything is the same (for security check purposes) as the current request except the HTTP method and the authorization
        ParsedS3Request checkRequest = new ParsedS3Request(
                request.requestId(),
                signingContext.signingAuthorization(),
                request.requestDate(),
                request.bucketName(),
                request.keyInBucket(),
                request.requestHeaders(),
                request.queryParameters(),
                httpMethod,
                request.rawPath(),
                request.rawQuery(),
                request.requestContent());

        return switch (s3SecurityController.apply(checkRequest)) {
            case Success _ -> Stream.of(Map.entry(httpMethod, signingContext.signingUri()));
            case Failure _ -> Stream.empty();
        };
    }
}
