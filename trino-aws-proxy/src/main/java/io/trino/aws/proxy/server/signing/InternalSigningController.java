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
import io.trino.aws.proxy.server.credentials.CredentialsController;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningContext;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class InternalSigningController
        implements SigningController
{
    private static final Logger log = Logger.get(SigningController.class);

    private final Duration maxClockDrift;
    private final RequestLoggerController requestLoggerController;
    private final CredentialsController credentialsController;

    private static final Set<String> LOWERCASE_HEADERS = ImmutableSet.of("content-type");

    @Inject
    public InternalSigningController(CredentialsController credentialsController, SigningControllerConfig signingControllerConfig, RequestLoggerController requestLoggerController)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
        this.requestLoggerController = requireNonNull(requestLoggerController, "requestLoggerController is null");

        maxClockDrift = signingControllerConfig.getMaxClockDrift().toJavaTime();
    }

    // temporary - remove once Airlift has been updated
    private enum Mode
    {
        ADJUSTED_HEADERS,
        UNADJUSTED_HEADERS,
    }

    @Override
    public RequestAuthorization signRequest(
            SigningMetadata metadata,
            String region,
            Instant requestDate,
            Optional<Instant> signatureExpiry,
            Function<Credentials, Credential> credentialsSupplier,
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
                credentialsSupplier,
                requestURI,
                SigningHeaders.build(requestHeaders),
                queryParameters,
                httpMethod).signingAuthorization();
    }

    @Override
    public SigningMetadata validateAndParseAuthorization(Request request, SigningServiceType signingServiceType)
    {
        if (!request.requestAuthorization().isValid()) {
            log.debug("Invalid requestAuthorization. Request: %s, SigningServiceType: %s", request, signingServiceType);
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        return credentialsController.withCredentials(request.requestAuthorization().accessKey(), request.requestAuthorization().securityToken(), credentials -> {
            SigningMetadata metadata = new SigningMetadata(signingServiceType, credentials, Optional.empty());
            return isValidAuthorization(metadata, request, Credentials::emulated);
        }).orElseThrow(() -> {
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
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            SigningHeaders signingHeaders,
            MultiMap queryParameters,
            String httpMethod)
    {
        Credential credential = credentialsSupplier.apply(metadata.credentials());

        Optional<byte[]> entity = metadata.signingServiceType().contentIsSigned() ? requestContent.standardBytes() : Optional.empty();

        return signatureExpiry.map(expiry -> Signer.presign(
                metadata.signingServiceType().serviceName(),
                requestURI,
                signingHeaders,
                queryParameters,
                region,
                requestDate,
                expiry,
                httpMethod,
                credential,
                maxClockDrift,
                entity)
        ).orElseGet(() -> Signer.sign(
                metadata.signingServiceType().serviceName(),
                requestURI,
                signingHeaders,
                queryParameters,
                region,
                requestDate,
                httpMethod,
                credential,
                maxClockDrift,
                entity));
    }

    @SuppressWarnings("resource")
    private Optional<SigningMetadata> isValidAuthorization(
            SigningMetadata metadata,
            Request request,
            Function<Credentials, Credential> credentialsSupplier)
    {
        // temp workaround until https://github.com/airlift/airlift/pull/1178 is accepted
        return Stream.of(Mode.values()).flatMap(mode -> {
            SigningHeaders signingHeaders = SigningHeaders.build(adjustHeaders(mode, request.requestHeaders()), request.requestAuthorization().lowercaseSignedHeaders());
            SigningContext signingContext = internalSignRequest(
                    metadata,
                    request.requestAuthorization().region(),
                    request.requestDate(),
                    request.requestAuthorization().expiry(),
                    request.requestContent(),
                    credentialsSupplier,
                    request.requestUri(),
                    signingHeaders,
                    request.requestQueryParameters(),
                    request.httpVerb());

            boolean generatedMatchesRequest = request.requestAuthorization().equals(signingContext.signingAuthorization());
            if (generatedMatchesRequest) {
                return Stream.of(metadata.withSigningContext(signingContext));
            }

            requestLoggerController.currentRequestSession(request.requestId())
                    .logError("request.security.authorization.mismatch", ImmutableMap.of("request", request.requestAuthorization(), "generated", signingContext.signingAuthorization()));
            return Stream.of();
        }).findFirst();
    }

    private static String lowercaseHeader(String headerName, String headerValue)
    {
        if (!LOWERCASE_HEADERS.contains(headerName)) {
            return headerValue;
        }
        return headerValue.toLowerCase(Locale.ROOT);
    }

    private static MultiMap adjustHeaders(Mode mode, MultiMap headers)
    {
        if (mode == Mode.UNADJUSTED_HEADERS) {
            return headers;
        }
        ImmutableMultiMap.Builder adjustedHeaderBuilder = ImmutableMultiMap.builder(headers.isCaseSensitiveKeys());
        headers.forEachEntry((headerName, headerValue) -> adjustedHeaderBuilder.add(headerName, lowercaseHeader(headerName, headerValue)));
        return adjustedHeaderBuilder.build();
    }
}
