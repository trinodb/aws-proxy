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

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.internal.AbstractAwsS3V4Signer;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
import software.amazon.awssdk.auth.signer.internal.CopiedAbstractAwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import java.net.URI;
import java.util.Optional;

class Signers
{
    static final String OVERRIDE_CONTENT_HASH = "__TRINO__OVERRIDE_CONTENT_HASH__";

    static final SigningApi aws4Signer = new InternalAwsS3V4Signer();

    static final SigningApi legacyAws4Signer = new InternalLegacyAwsS3V4Signer();

    private static final URI DUMMY_URI = URI.create("https://local.gate0.net");

    interface SigningApi
    {
        SdkHttpFullRequest sign(SdkHttpFullRequest request, AwsS3V4SignerParams signingParams);

        SdkHttpFullRequest presign(SdkHttpFullRequest request, Aws4PresignerParams signingParams);

        byte[] signingKey(AwsCredentials credentials, Aws4SignerRequestParams signerRequestParams);
    }

    private Signers() {}

    private static class InternalAwsS3V4Signer
            extends AbstractAwsS3V4Signer
            implements SigningApi
    {
        @Override
        public byte[] signingKey(AwsCredentials credentials, Aws4SignerRequestParams signerRequestParams)
        {
            return deriveSigningKey(credentials, signerRequestParams);
        }

        @Override
        protected String calculateContentHash(SdkHttpFullRequest.Builder mutableRequest, AwsS3V4SignerParams signerParams, SdkChecksum contentFlexibleChecksum)
        {
            return extractOverrideContentHash(mutableRequest).orElseGet(() -> super.calculateContentHash(mutableRequest, signerParams, contentFlexibleChecksum));
        }
    }

    private static class InternalLegacyAwsS3V4Signer
            extends CopiedAbstractAwsS3V4Signer
            implements SigningApi
    {
        @Override
        public byte[] signingKey(AwsCredentials credentials, Aws4SignerRequestParams signerRequestParams)
        {
            return deriveSigningKey(credentials, signerRequestParams);
        }

        @Override
        protected String calculateContentHash(SdkHttpFullRequest.Builder mutableRequest, AwsS3V4SignerParams signerParams, SdkChecksum contentFlexibleChecksum)
        {
            return extractOverrideContentHash(mutableRequest).orElseGet(() -> super.calculateContentHash(mutableRequest, signerParams, contentFlexibleChecksum));
        }
    }

    private static Optional<String> extractOverrideContentHash(SdkHttpFullRequest.Builder mutableRequest)
    {
        // look for the stashed OVERRIDE_CONTENT_HASH, remove the hacked header and then return the stashed hash value
        return mutableRequest.firstMatchingHeader(OVERRIDE_CONTENT_HASH).map(hash -> {
            mutableRequest.removeHeader(OVERRIDE_CONTENT_HASH);
            return hash;
        });
    }
}
