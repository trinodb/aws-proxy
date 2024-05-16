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

import com.google.inject.Inject;
import io.trino.s3.proxy.server.minio.Signer;
import io.trino.s3.proxy.server.minio.emulation.MinioRequest;
import io.trino.s3.proxy.server.minio.emulation.MinioUrl;
import jakarta.ws.rs.core.MultivaluedMap;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SigningController
{
    private final CredentialsController credentialsController;

    @Inject
    public SigningController(CredentialsController credentialsController)
    {
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
    }

    public Map<String, String> signedRequestHeaders(String method, MultivaluedMap<String, String> requestHeaders, String encodedPath, String encodedQuery, String region, String accessKey)
    {
        // TODO
        Credentials credentials = credentialsController.credentials(accessKey).orElseThrow();

        MinioUrl minioUrl = MinioUrl.build(encodedPath, encodedQuery);
        MinioRequest minioRequest = MinioRequest.build(requestHeaders, method, minioUrl);

        // TODO
        String sha256 = minioRequest.headerValue("x-amz-content-sha256").orElseThrow();

        try {
            return Signer.signV4S3(minioRequest, region, accessKey, credentials.emulatedSecretKey(), sha256).headers();
        }
        catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
