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
import jakarta.ws.rs.core.MultivaluedMap;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.s3.proxy.server.credentials.Signer.AMZ_DATE_FORMAT;
import static io.trino.s3.proxy.server.credentials.Signer.UTC;
import static java.util.Objects.requireNonNull;

public class SigningController
{
    private final CredentialsController credentialsController;
    private final Duration maxClockDrift;

    @Inject
    public SigningController(CredentialsController credentialsController, SigningControllerConfig signingControllerConfig)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
        maxClockDrift = signingControllerConfig.getMaxClockDrift().toJavaTime();
    }

    public static String formatRequestInstant(Instant instant)
    {
        return instant.atZone(UTC).format(AMZ_DATE_FORMAT);
    }

    public Optional<SigningMetadata> signingMetadataFromRequest(
            Function<Credentials, Credentials.Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            String encodedPath)
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

        return credentialsController.credentials(emulatedAccessKey, session)
                .map(credentials -> new SigningMetadata(credentials, session, region))
                .filter(metadata -> isValidAuthorization(metadata, credentialsSupplier, authorization, requestURI, requestHeaders, queryParameters, httpMethod, encodedPath));
    }

    public String signRequest(
            SigningMetadata metadata,
            Function<Credentials, Credentials.Credential> credentialsSupplier,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            String encodedPath)
    {
        Credentials.Credential credential = credentialsSupplier.apply(metadata.credentials());

        return Signer.sign(
                "s3",
                requestURI,
                requestHeaders,
                queryParameters,
                httpMethod,
                encodedPath,
                metadata.region(),
                credential.accessKey(),
                credential.secretKey(),
                maxClockDrift,
                Optional.empty());
    }

    private boolean isValidAuthorization(
            SigningMetadata metadata,
            Function<Credentials, Credentials.Credential> credentialsSupplier,
            String authorizationHeader,
            URI requestURI,
            MultivaluedMap<String, String> requestHeaders,
            MultivaluedMap<String, String> queryParameters,
            String httpMethod,
            String encodedPath)
    {
        String expectedAuthorization = signRequest(metadata, credentialsSupplier, requestURI, requestHeaders, queryParameters, httpMethod, encodedPath);
        return authorizationHeader.equals(expectedAuthorization);
    }
}
