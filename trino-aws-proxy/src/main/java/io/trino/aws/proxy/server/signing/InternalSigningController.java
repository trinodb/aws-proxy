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
package io.trino.aws.proxy.server.signing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.signing.SigningContext;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class InternalSigningController
        implements SigningController
{
    private static final Logger log = Logger.get(SigningController.class);

    private final Duration maxClockDrift;
    private final RequestLoggerController requestLoggerController;
    private final CredentialsProvider credentialsProvider;

    private static final Set<String> LOWERCASE_HEADERS = ImmutableSet.of("content-type");

    @Inject
    public InternalSigningController(CredentialsProvider credentialsProvider, SigningControllerConfig signingControllerConfig, RequestLoggerController requestLoggerController)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "signingCredentialProvider is null");
        this.requestLoggerController = requireNonNull(requestLoggerController, "requestLoggerController is null");

        maxClockDrift = signingControllerConfig.getMaxClockDrift().toJavaTime();
    }

    @Override
    public SigningContext signRequest(
            SigningMetadata metadata,
            String region,
            Instant requestDate,
            Optional<Instant> signatureExpiry,
            URI requestURI,
            MultiMap requestHeaders,
            MultiMap queryParameters,
            String httpMethod)
    {
        return internalSignRequest(
                metadata,
                region,
                requestDate,
                signatureExpiry,
                RequestContent.EMPTY,
                requestURI,
                SigningHeaders.build(requestHeaders),
                queryParameters,
                httpMethod);
    }

    @Override
    public SigningContext presignRequest(
            SigningMetadata metadata,
            String region,
            Instant requestDate,
            Optional<Instant> signatureExpiry,
            URI requestURI,
            MultiMap queryParameters,
            String httpMethod)
    {
        return internalSignRequest(
                metadata,
                region,
                requestDate,
                signatureExpiry,
                RequestContent.EMPTY,
                requestURI,
                SigningHeaders.EMPTY,
                queryParameters,
                httpMethod);
    }

    @Override
    public SigningIdentity validateAndParseAuthorization(Request request, SigningServiceType signingServiceType)
    {
        if (!request.requestAuthorization().isValid()) {
            log.debug("Invalid requestAuthorization. Request: %s, SigningServiceType: %s", request, signingServiceType);
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        return credentialsProvider.credentials(request.requestAuthorization().accessKey(), request.requestAuthorization().securityToken())
                .flatMap(identityCredential ->
                        isValidAuthorization(new SigningMetadata(signingServiceType, identityCredential.emulated(), Optional.empty()), request)
                                .map(signingMetadata -> new SigningIdentity(signingMetadata, identityCredential.identity())))
                .orElseThrow(() -> {
                    log.debug("ValidateAndParseAuthorization failed. Request: %s, SigningServiceType: %s", request, signingServiceType);
                    return new WebApplicationException(Response.Status.UNAUTHORIZED);
                });
    }

    private SigningContext internalSignRequest(
            SigningMetadata metadata,
            String region,
            Instant requestDate,
            Optional<Instant> signatureExpiry,
            RequestContent requestContent,
            URI requestURI,
            SigningHeaders signingHeaders,
            MultiMap queryParameters,
            String httpMethod)
    {
        Credential credential = metadata.credential();

        return signatureExpiry.map(expiry -> Signer.presign(
                metadata.signingServiceType(),
                requestURI,
                signingHeaders,
                queryParameters,
                region,
                requestDate,
                expiry,
                httpMethod,
                credential,
                maxClockDrift,
                requestContent)
        ).orElseGet(() -> Signer.sign(
                metadata.signingServiceType(),
                requestURI,
                signingHeaders,
                queryParameters,
                region,
                requestDate,
                httpMethod,
                credential,
                maxClockDrift,
                requestContent));
    }

    @SuppressWarnings("resource")
    private Optional<SigningMetadata> isValidAuthorization(
            SigningMetadata metadata,
            Request request)
    {
        SigningHeaders signingHeaders = SigningHeaders.build(request.requestHeaders().unmodifiedHeaders(), request.requestAuthorization().lowercaseSignedHeaders());
        SigningContext signingContext = internalSignRequest(
                metadata,
                request.requestAuthorization().region(),
                request.requestDate(),
                request.requestAuthorization().expiry(),
                request.requestContent(),
                request.requestUri(),
                signingHeaders,
                request.requestQueryParameters(),
                request.httpVerb());

        boolean generatedMatchesRequest = request.requestAuthorization().equals(signingContext.signingAuthorization());
        if (generatedMatchesRequest) {
            return Optional.of(metadata.withSigningContext(signingContext));
        }

        requestLoggerController.currentRequestSession(request.requestId())
                .logError("request.security.authorization.mismatch", ImmutableMap.of("request", request.requestAuthorization(), "generated",
                        signingContext.signingAuthorization()));
        return Optional.empty();
    }
}
