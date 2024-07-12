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

import com.google.common.hash.HashCode;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import software.amazon.awssdk.auth.signer.internal.AbstractAws4Signer;
import software.amazon.awssdk.auth.signer.internal.SigningAlgorithm;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.utils.BinaryUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

/**
 * Extracted/copied from {@link AwsS3V4ChunkSigner} and <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html">sigv4-streaming</a>.
 * We don't want to use {@link AwsS3V4ChunkSigner} directly as it works with {@code byte[]} and doesn't allow for streaming.
 */
class ChunkSigner
{
    private static final String CHUNK_STRING_TO_SIGN_PREFIX = "AWS4-HMAC-SHA256-PAYLOAD";

    private final String dateTime;
    private final String keyPath;
    private final Mac hmacSha256;

    ChunkSigner(Instant requestDate, String keyPath, byte[] signingKey)
    {
        this.dateTime = AwsTimestamp.toRequestFormat(requestDate);
        this.keyPath = keyPath;

        try {
            String signingAlgorithm = SigningAlgorithm.HmacSHA256.toString();
            this.hmacSha256 = Mac.getInstance(signingAlgorithm);
            hmacSha256.init(new SecretKeySpec(signingKey, signingAlgorithm));
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        catch (InvalidKeyException e) {
            throw new IllegalArgumentException(e);
        }
    }

    String signChunk(HashCode hashCode, String previousSignature)
    {
        String chunkStringToSign =
                CHUNK_STRING_TO_SIGN_PREFIX + "\n" +
                        dateTime + "\n" +
                        keyPath + "\n" +
                        previousSignature + "\n" +
                        AbstractAws4Signer.EMPTY_STRING_SHA256_HEX + "\n" +
                        hashCode.toString();
        try {
            byte[] bytes = hmacSha256.doFinal(chunkStringToSign.getBytes(StandardCharsets.UTF_8));
            return BinaryUtils.toHex(bytes);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not sign chunk", e);
        }
    }
}
