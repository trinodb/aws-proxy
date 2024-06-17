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
package io.trino.s3.proxy.server.rest;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import jakarta.ws.rs.WebApplicationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnstableApiUsage")
class HashCheckInputStream
        extends InputStream
{
    private static final Logger log = Logger.get(HashCheckInputStream.class);

    private final InputStream delegate;
    private final String expectedHash;
    private final Optional<Integer> expectedLength;
    private final Hasher hasher;

    private boolean hasBeenValidated;
    private int bytesRead;

    HashCheckInputStream(InputStream delegate, String expectedHash, Optional<Integer> expectedLength)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.expectedHash = requireNonNull(expectedHash, "expectedHash is null");
        this.expectedLength = requireNonNull(expectedLength, "expectedLength is null");

        hasher = Hashing.sha256().newHasher();
    }

    @Override
    public int read()
            throws IOException
    {
        int i = delegate.read();
        if (i < 0) {
            validateHash();
            return i;
        }

        hasher.putByte((byte) (i & 0xff));
        updateBytesRead(1);

        return i;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        int result = delegate.read(b, off, len);
        if (result < 0) {
            validateHash();
            return result;
        }

        hasher.putBytes(b, off, result);
        updateBytesRead(result);

        return result;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private void validateHash()
    {
        if (hasBeenValidated) {
            return;
        }
        hasBeenValidated = true;

        String actualHash = hasher.hash().toString();
        if (!actualHash.equals(expectedHash)) {
            log.debug("Actual hash does not match expected hash. Expected: %s, Actual: %s", expectedHash, actualHash);
            throw new WebApplicationException(UNAUTHORIZED);
        }
    }

    private void updateBytesRead(int count)
    {
        bytesRead += count;
        expectedLength.ifPresent(expected -> {
            if (bytesRead > expected) {
                log.debug("More bytes read than expected. Expected: %s, Actual: %s", expected, bytesRead);
                throw new WebApplicationException(UNAUTHORIZED);
            }

            if (bytesRead == expected) {
                validateHash();
            }
        });
    }
}
