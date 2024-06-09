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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.s3.proxy.server.signing.Signer.AMZ_DATE_FORMAT;
import static io.trino.s3.proxy.server.signing.Signer.RESPONSE_DATE_FORMAT;
import static io.trino.s3.proxy.server.signing.Signer.ZONE;
import static io.trino.s3.proxy.server.signing.SigningController.Mode.UNADJUSTED_HEADERS;
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
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            Optional<byte[]> entity)
    {
        return internalSignRequest(
                UNADJUSTED_HEADERS,
                metadata,
                credentialsSupplier,
                requestURI,
                requestHeaders,
                queryParameters,
                httpMethod,
                entity);
    }

    public SigningMetadata validateAndParseAuthorization(Request request, SigningServiceType signingServiceType, Optional<byte[]> entity)
    {
        return signingMetadataFromRequest(
                signingServiceType,
                Credentials::emulated,
                request.requestUri(),
                request.requestHeaders(),
                request.requestQueryParameters(),
                request.httpVerb(),
                entity)
                .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));
    }

    private String internalSignRequest(
            Mode mode,
            SigningMetadata metadata,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            Optional<byte[]> entity)
    {
        Credential credential = credentialsSupplier.apply(metadata.credentials());

        return Signer.sign(
                mode,
                metadata.signingServiceType().serviceName(),
                requestURI,
                requestHeaders,
                queryParameters,
                httpMethod,
                metadata.region(),
                credential.accessKey(),
                credential.secretKey(),
                maxClockDrift,
                entity);
    }

    private Optional<SigningMetadata> signingMetadataFromRequest(
            SigningServiceType signingServiceType,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            Optional<byte[]> entity)
    {
        ParsedAuthorization parsedAuthorization = ParsedAuthorization.parse(firstNonNull(requestHeaders.getFirst("authorization"), ""));
        if (!parsedAuthorization.isValid()) {
            return Optional.empty();
        }

        Optional<String> session = Optional.ofNullable(requestHeaders.getFirst("x-amz-security-token"));

        return credentialsController.withCredentials(parsedAuthorization.accessKey(), session, credentials -> {
            SigningMetadata metadata = new SigningMetadata(signingServiceType, credentials, session, parsedAuthorization.region());
            if (isValidAuthorization(metadata, credentialsSupplier, parsedAuthorization.authorization(), requestURI, requestHeaders, queryParameters, httpMethod, entity)) {
                return Optional.of(metadata);
            }
            return Optional.empty();
        });
    }

    private boolean isValidAuthorization(
            SigningMetadata metadata,
            Function<Credentials, Credential> credentialsSupplier,
            String authorizationHeader,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            Optional<byte[]> entity)
    {
        // temp workaround until https://github.com/airlift/airlift/pull/1178 is accepted
        return Stream.of(Mode.values()).anyMatch(mode -> {
            String expectedAuthorization = internalSignRequest(mode, metadata, credentialsSupplier, requestURI, requestHeaders, queryParameters, httpMethod, entity);
            return authorizationHeader.equals(expectedAuthorization);
        });
    }
}
