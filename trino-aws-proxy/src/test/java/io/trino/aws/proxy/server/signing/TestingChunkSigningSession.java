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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.trino.aws.proxy.spi.credentials.Credential;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.regions.Region;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TestingChunkSigningSession
        extends InternalChunkSigningSession
{
    private static final DateTimeFormatter CHUNK_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US).withZone(ZoneId.of("Z"));

    private final String seed;

    public static TestingChunkSigningSession build()
    {
        Credential credential = new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String seed = "0".repeat(AwsS3V4ChunkSigner.getSignatureLength());
        return build(credential, seed);
    }

    public static TestingChunkSigningSession build(Credential credential, String seed)
    {
        return build(credential, seed, Instant.now());
    }

    public static TestingChunkSigningSession build(Credential credential, String seed, Instant instant)
    {
        AwsCredentials credentials = AwsBasicCredentials.create(credential.accessKey(), credential.secretKey());
        Aws4SignerParams.Builder<?> builder = Aws4SignerParams.builder()
                .awsCredentials(credentials)
                .doubleUrlEncode(false)
                .signingName("s3")
                .signingRegion(Region.US_EAST_1);
        byte[] signingKey = Signer.signingKey(credentials, new Aws4SignerRequestParams(builder.build()));

        return new TestingChunkSigningSession(seed, instant, signingKey, "%s/us-east-1/s3/aws4_request".formatted(CHUNK_DATETIME_FORMAT.format(instant)));
    }

    public static int getExpectedChunkedStreamSize(String rawContent, int partitions)
    {
        int contentSizeInBytes = rawContent.getBytes(UTF_8).length;
        int standardChunkSize = Math.ceilDiv(contentSizeInBytes, partitions);
        // The penultimate chunk may be smaller if alignment is not perfect
        int penultimateChunkSize = contentSizeInBytes - (standardChunkSize * (partitions - 1));
        // Each chunk has:
        //   - A header consisting of "<size>;chunk-signature=<signature>"
        //   - \r\n
        //   - <data>
        //   - \r\n
        int baseChunkSize = ";chunk-signature=".length() + AwsS3V4ChunkSigner.getSignatureLength() + 4;
        // Chunk headers without including the size
        return (baseChunkSize * (partitions + 1)) +
                // Size of the size field for all chunk except for the last 2
                (Integer.toString(standardChunkSize, 16).length() * (partitions - 1)) +
                // Size of the size field for the penultimate chunk
                (Integer.toString(penultimateChunkSize, 16).length()) +
                // Size of the size field for the last chunk (size=0, 1 character)
                1 +
                // Size of the actual content
                contentSizeInBytes;
    }

    @SuppressWarnings("UnstableApiUsage")
    public String generateChunkedStream(String content, int partitions)
    {
        checkArgument(partitions > 1, "partitions must be greater than 1");

        String previousSignature = seed;

        StringBuilder chunkedStream = new StringBuilder();
        int chunkSize = Math.ceilDiv(content.length(), partitions);
        int index = 0;
        while (index < content.length()) {
            int thisLength = Math.min(chunkSize, content.length() - index);
            String thisChunk = content.substring(index, index + thisLength);

            String thisSignature = getChunkSignature(thisChunk, previousSignature);
            chunkedStream.append(Integer.toHexString(thisLength)).append(";chunk-signature=").append(thisSignature).append("\r\n");
            chunkedStream.append(thisChunk).append("\r\n");
            previousSignature = thisSignature;

            index += thisLength;
        }

        String thisSignature = chunkSigner.signChunk(Hashing.sha256().newHasher().hash(), previousSignature);
        chunkedStream.append("0;chunk-signature=").append(thisSignature).append("\r\n\r\n");

        return chunkedStream.toString();
    }

    @SuppressWarnings("UnstableApiUsage")
    public String getChunkSignature(String chunkContent, String previousSignature)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putString(chunkContent, UTF_8);
        return chunkSigner.signChunk(hasher.hash(), previousSignature);
    }

    private TestingChunkSigningSession(String seed, Instant instant, byte[] signingKey, String keyPath)
    {
        super(new ChunkSigner(instant, keyPath, signingKey), seed);

        this.seed = requireNonNull(seed, "seed is null");
    }
}
