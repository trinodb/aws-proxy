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
package io.trino.s3.proxy.server.credentials;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.s3.proxy.server.credentials.Signer.AMZ_DATE_FORMAT;
import static io.trino.s3.proxy.server.credentials.Signer.RESPONSE_DATE_FORMAT;
import static io.trino.s3.proxy.server.credentials.Signer.ZONE;
import static java.util.Objects.requireNonNull;

public class SigningController
{
    private final CredentialsProvider credentialsProvider;
    private final Duration maxClockDrift;

    @Inject
    public SigningController(CredentialsProvider credentialsProvider, SigningControllerConfig signingControllerConfig)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsController is null");
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

    public String signRequest(
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
                metadata.signingServiceType().asServiceName(),
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

    public SigningMetadata validateAndParseAuthorization(ContainerRequest request, SigningServiceType signingServiceType, Optional<byte[]> entity)
    {
        return signingMetadataFromRequest(
                signingServiceType,
                Credentials::emulated,
                request.getRequestUri(),
                request.getRequestHeaders(),
                request.getUriInfo().getQueryParameters(),
                request.getMethod(),
                entity)
                .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));
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
        String authorization = requestHeaders.getFirst("Authorization");
        if (authorization == null) {
            return Optional.empty();
        }

        List<String> authorizationParts = Splitter.on(",").trimResults().splitToList(authorization);
        if (authorizationParts.isEmpty()) {
            return Optional.empty();
        }

        String credential = authorizationParts.getFirst();
        List<String> credentialParts = Splitter.on("=").splitToList(credential);
        if (credentialParts.size() < 2) {
            return Optional.empty();
        }

        String credentialValue = credentialParts.get(1);
        List<String> credentialValueParts = Splitter.on("/").splitToList(credentialValue);
        if (credentialValueParts.size() < 3) {
            return Optional.empty();
        }

        String emulatedAccessKey = credentialValueParts.getFirst();
        String region = credentialValueParts.get(2);

        Optional<String> session = Optional.ofNullable(requestHeaders.getFirst("x-amz-security-token"));

        return credentialsProvider.credentials(emulatedAccessKey, session)
                .map(credentials -> new SigningMetadata(signingServiceType, credentials, session, region))
                .filter(metadata -> isValidAuthorization(metadata, credentialsSupplier, authorization, requestURI, requestHeaders, queryParameters, httpMethod, entity));
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
        String expectedAuthorization = signRequest(metadata, credentialsSupplier, requestURI, requestHeaders, queryParameters, httpMethod, entity);
        return authorizationHeader.equals(expectedAuthorization);
    }
}
