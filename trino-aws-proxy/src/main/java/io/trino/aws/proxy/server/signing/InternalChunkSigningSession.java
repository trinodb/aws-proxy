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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.signing.ChunkSigningSession;
import jakarta.ws.rs.WebApplicationException;

import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnstableApiUsage")
class InternalChunkSigningSession
        implements ChunkSigningSession
{
    private static final Logger log = Logger.get(InternalChunkSigningSession.class);

    @VisibleForTesting
    protected final ChunkSigner chunkSigner;
    private String previousSignature;
    private String expectedSignature;
    private Hasher hasher;

    public InternalChunkSigningSession(ChunkSigner chunkSigner, String seed)
    {
        this.chunkSigner = requireNonNull(chunkSigner, "chunkSigner is null");
        previousSignature = requireNonNull(seed, "seed is null");
    }

    @Override
    public void startChunk(String expectedSignature)
    {
        complete();

        hasher = Hashing.sha256().newHasher();
        this.expectedSignature = requireNonNull(expectedSignature, "expectedSignature is null");
    }

    @Override
    public void complete()
    {
        if ((hasher == null) || (expectedSignature == null)) {
            return;
        }

        String thisSignature = chunkSigner.signChunk(hasher.hash(), previousSignature);
        if (!thisSignature.equals(expectedSignature)) {
            log.debug("Chunk signature does not match expected signature. Expected: %s, Actual: %s", expectedSignature, thisSignature);
            throw new WebApplicationException(UNAUTHORIZED);
        }
        previousSignature = expectedSignature;

        hasher = null;
        expectedSignature = null;
    }

    @Override
    public void write(byte b)
    {
        hasher.putByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        hasher.putBytes(b, off, len);
    }
}
