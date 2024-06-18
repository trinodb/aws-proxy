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
package io.trino.s3.proxy.server.signing;

import com.google.inject.Inject;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.CredentialsController;
import io.trino.s3.proxy.server.rest.Request;
import io.trino.s3.proxy.server.rest.RequestContent;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.s3.proxy.server.signing.Signer.AMZ_DATE_FORMAT;
import static io.trino.s3.proxy.server.signing.Signer.RESPONSE_DATE_FORMAT;
import static io.trino.s3.proxy.server.signing.Signer.ZONE;
import static java.util.Objects.requireNonNull;

public class SigningController
{
    private final Duration maxClockDrift;
    private final CredentialsController credentialsController;

    @Inject
    public SigningController(CredentialsController credentialsController, SigningControllerConfig signingControllerConfig)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
        maxClockDrift = signingControllerConfig.getMaxClockDrift().toJavaTime();
    }

    public static String formatRequestInstant(Instant instant)
    {
        return instant.atZone(ZONE).format(AMZ_DATE_FORMAT);
    }

    public static String formatResponseInstant(Instant instant)
    {
        return instant.atZone(ZONE).format(RESPONSE_DATE_FORMAT);
    }

    // temporary - remove once Airlift has been updated
    public enum Mode
    {
        ADJUSTED_HEADERS,
        UNADJUSTED_HEADERS,
    }

    public String signRequest(
            SigningMetadata metadata,
            String region,
            String requestDate,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod)
    {
        return internalSignRequest(
                metadata,
                region,
                requestDate,
                RequestContent.EMPTY,
                credentialsSupplier,
                requestURI,
                SigningHeaders.build(requestHeaders),
                queryParameters,
                httpMethod).signingAuthorization().authorization();
    }

    public SigningMetadata validateAndParseAuthorization(Request request, SigningServiceType signingServiceType)
    {
        if (!request.requestAuthorization().isValid()) {
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        return credentialsController.withCredentials(request.requestAuthorization().accessKey(), request.requestAuthorization().securityToken(), credentials -> {
            SigningMetadata metadata = new SigningMetadata(signingServiceType, credentials, Optional.empty());
            return isValidAuthorization(metadata, request, Credentials::emulated);
        }).orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));
    }

    private SigningContext internalSignRequest(
            SigningMetadata metadata,
            String region,
            String requestDate,
            RequestContent requestContent,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            SigningHeaders signingHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod)
    {
        Credential credential = credentialsSupplier.apply(metadata.credentials());

        Optional<byte[]> entity = metadata.signingServiceType().contentIsSigned() ? requestContent.standardBytes() : Optional.empty();

        return Signer.sign(
                metadata.signingServiceType().serviceName(),
                requestURI,
                signingHeaders,
                queryParameters,
                region,
                requestDate,
                httpMethod,
                credential.accessKey(),
                credential.secretKey(),
                maxClockDrift,
                entity);
    }

    private Optional<SigningMetadata> isValidAuthorization(
            SigningMetadata metadata,
            Request request,
            Function<Credentials, Credential> credentialsSupplier)
    {
        // temp workaround until https://github.com/airlift/airlift/pull/1178 is accepted
        return Stream.of(Mode.values()).flatMap(mode -> {
            SigningHeaders signingHeaders = SigningHeaders.build(mode, request.requestHeaders(), request.requestAuthorization().lowercaseSignedHeaders());
            SigningContext signingContext = internalSignRequest(
                    metadata,
                    request.requestAuthorization().region(),
                    request.requestDate(),
                    request.requestContent(),
                    credentialsSupplier,
                    request.requestUri(),
                    signingHeaders,
                    request.requestQueryParameters(),
                    request.httpVerb());
            return request.requestAuthorization().authorization().equals(signingContext.signingAuthorization().authorization()) ? Stream.of(metadata.withSigningContext(signingContext)) : Stream.of();
        }).findFirst();
    }
}
