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
            String httpMethod)
    {
        SigningHeaders signingHeaders = SigningHeaders.build(requestHeaders);

        SigningContext signingContext = internalSignRequest(
                metadata,
                credentialsSupplier,
                requestURI,
                signingHeaders,
                queryParameters,
                httpMethod);
        return signingContext.authorization();
    }

    public SigningMetadata validateAndParseAuthorization(Request request, SigningServiceType signingServiceType)
    {
        ParsedAuthorization parsedAuthorization = ParsedAuthorization.parse(firstNonNull(request.requestHeaders().getFirst("authorization"), ""));
        if (!parsedAuthorization.isValid()) {
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        Optional<String> session = Optional.ofNullable(request.requestHeaders().getFirst("x-amz-security-token"));

        return credentialsController.withCredentials(parsedAuthorization.accessKey(), session, credentials -> {
            SigningMetadata metadata = new SigningMetadata(signingServiceType, credentials, session, parsedAuthorization.region(), request.requestContent());
            return isValidAuthorization(metadata, Credentials::emulated, parsedAuthorization, request.requestUri(), request.requestHeaders(), request.requestQueryParameters(), request.httpVerb());
        }).orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));
    }

    private SigningContext internalSignRequest(
            SigningMetadata metadata,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            SigningHeaders signingHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod)
    {
        Credential credential = credentialsSupplier.apply(metadata.credentials());

        Optional<byte[]> entity = metadata.signingServiceType().contentIsSigned() ? metadata.requestContent().standardBytes() : Optional.empty();

        return Signer.sign(
                metadata.signingServiceType().serviceName(),
                requestURI,
                signingHeaders,
                queryParameters,
                httpMethod,
                metadata.region(),
                credential.accessKey(),
                credential.secretKey(),
                maxClockDrift,
                entity);
    }

    private Optional<SigningMetadata> isValidAuthorization(
            SigningMetadata metadata,
            Function<Credentials, Credential> credentialsSupplier,
            ParsedAuthorization parsedAuthorization,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod)
    {
        // temp workaround until https://github.com/airlift/airlift/pull/1178 is accepted
        return Stream.of(Mode.values()).flatMap(mode -> {
            SigningHeaders signingHeaders = SigningHeaders.build(mode, requestHeaders, parsedAuthorization.signedLowercaseHeaders());
            SigningContext signingContext = internalSignRequest(metadata, credentialsSupplier, requestURI, signingHeaders, queryParameters, httpMethod);
            return parsedAuthorization.authorization().equals(signingContext.authorization()) ? Stream.of(metadata.withSigningContext(signingContext)) : Stream.of();
        }).findFirst();
    }
}
