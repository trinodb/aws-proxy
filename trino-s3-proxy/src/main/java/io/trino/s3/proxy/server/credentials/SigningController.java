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
import io.trino.s3.proxy.server.minio.Signer;
import io.trino.s3.proxy.server.minio.emulation.MinioRequest;
import io.trino.s3.proxy.server.minio.emulation.MinioUrl;
import jakarta.ws.rs.core.MultivaluedMap;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SigningController
{
    private final CredentialsController credentialsController;

    @Inject
    public SigningController(CredentialsController credentialsController)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
    }

    public record Scope(String authorization, String accessKey, String region)
    {
        public Scope
        {
            authorization = requireNonNull(authorization, "accessKey is null");
            accessKey = requireNonNull(accessKey, "accessKey is null");
            region = requireNonNull(region, "region is null");
        }

        public static Optional<Scope> fromHeaders(MultivaluedMap<String, String> requestHeaders)
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

            String accessKey = credentialValueParts.getFirst();
            String region = credentialValueParts.get(2);
            return Optional.of(new Scope(authorization, accessKey, region));
        }
    }

    public boolean validateRequest(String method, MultivaluedMap<String, String> requestHeaders, String encodedPath, String encodedQuery)
    {
        return Scope.fromHeaders(requestHeaders).map(scope -> {
            Map<String, String> signedRequestHeaders = signedRequestHeaders(scope, method, requestHeaders, encodedPath, encodedQuery);
            String requestAuthorization = signedRequestHeaders.get("Authorization");
            return scope.authorization.equals(requestAuthorization);
        }).orElse(false);
    }

    public Map<String, String> signedRequestHeaders(Scope scope, String method, MultivaluedMap<String, String> requestHeaders, String encodedPath, String encodedQuery)
    {
        // TODO
        Credentials credentials = credentialsController.credentials(scope.accessKey).orElseThrow();

        MinioUrl minioUrl = MinioUrl.build(encodedPath, encodedQuery);
        MinioRequest minioRequest = MinioRequest.build(requestHeaders, method, minioUrl);

        // TODO
        String sha256 = minioRequest.headerValue("x-amz-content-sha256").orElseThrow();

        try {
            return Signer.signV4S3(minioRequest, scope.region, scope.accessKey, credentials.emulated().secretKey(), sha256).headers();
        }
        catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
